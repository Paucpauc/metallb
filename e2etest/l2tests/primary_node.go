/*
Copyright 2016 The Kubernetes Authors.
Copyright 2021 The MetalLB Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package l2tests

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metallbv1beta1 "go.universe.tf/metallb/api/v1beta1"
	"go.universe.tf/e2etest/pkg/k8s"
	"go.universe.tf/e2etest/pkg/k8sclient"
	"go.universe.tf/e2etest/pkg/service"
	"go.universe.tf/e2etest/pkg/config"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
)

const (
	primaryNodeAnnotation = "metallb.io" + "/" + "primary-node"
)

var _ = ginkgo.Describe("L2 Primary Node Annotation", func() {
	var cs clientset.Interface
	testNamespace := ""

	ginkgo.BeforeEach(func() {
		cs = k8sclient.New()
		var err error
		testNamespace, err = k8s.CreateTestNamespace(cs, "l2-primary")
		Expect(err).NotTo(HaveOccurred())

		resources := config.Resources{
			Pools: []metallbv1beta1.IPAddressPool{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "primary-test"},
					Spec: metallbv1beta1.IPAddressPoolSpec{
						Addresses: []string{IPV4ServiceRange, IPV6ServiceRange},
					},
				},
			},
			L2Advs: []metallbv1beta1.L2Advertisement{{ObjectMeta: metav1.ObjectMeta{Name: "empty"}}},
		}
		err = ConfigUpdater.Update(resources)
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		err := ConfigUpdater.Clean()
		Expect(err).NotTo(HaveOccurred())
		err = k8s.DeleteNamespace(cs, testNamespace)
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.It("should respect primary-node annotation", func() {
		nodes, err := cs.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())
		if len(nodes.Items) < 2 {
			ginkgo.Skip("Requires at least 2 nodes")
		}

		node1 := nodes.Items[0].Name
		node2 := nodes.Items[1].Name
		invalidNode := "non-existent-node"

		// Helper to get announcing node
		getAnnouncingNode := func(svc *corev1.Service) string {
			var node string
			Eventually(func() error {
				n, err := nodeForService(svc, nodes.Items)
				if err != nil {
					return err
				}
				node = n.Name
				return nil
			}, 2*time.Minute, 1*time.Second).ShouldNot(HaveOccurred())
			return node
		}

		// 1. Create service without annotation
		svc, _ := service.CreateWithBackend(cs, testNamespace, "primary-node-svc", service.TrafficPolicyCluster)
		defer func() {
			err := cs.CoreV1().Services(svc.Namespace).Delete(context.TODO(), svc.Name, metav1.DeleteOptions{})
			Expect(err).NotTo(HaveOccurred())
		}()

		ginkgo.By("Service should be announced by any available node")
		initialNode := getAnnouncingNode(svc)
		Expect(initialNode).NotTo(BeEmpty())

		// 2. Add annotation to node1
		ginkgo.By("Adding primary-node annotation to node1")

		currentSvc, err := cs.CoreV1().Services(testNamespace).Get(context.TODO(), svc.Name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		if currentSvc.Annotations == nil {
			currentSvc.Annotations = make(map[string]string)
		}
		currentSvc.Annotations[primaryNodeAnnotation] = node1

		_, err = cs.CoreV1().Services(testNamespace).Update(context.TODO(), currentSvc, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		ginkgo.By("Service should be announced by node1")
		Eventually(func() string {
			return getAnnouncingNode(currentSvc)
		}, time.Minute, 2*time.Second).Should(Equal(node1))

		// 3. Change annotation to node2
		ginkgo.By("Changing primary-node annotation to node2")

		currentSvc, err = cs.CoreV1().Services(testNamespace).Get(context.TODO(), svc.Name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		currentSvc.Annotations[primaryNodeAnnotation] = node2

		_, err = cs.CoreV1().Services(testNamespace).Update(context.TODO(), currentSvc, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		ginkgo.By("Service should be announced by node2")
		Eventually(func() string {
			return getAnnouncingNode(currentSvc)
		}, time.Minute, 2*time.Second).Should(Equal(node2))

		// 4. Set invalid node annotation
		ginkgo.By("Setting invalid primary-node annotation")

		currentSvc, err = cs.CoreV1().Services(testNamespace).Get(context.TODO(), svc.Name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		currentSvc.Annotations[primaryNodeAnnotation] = invalidNode

		_, err = cs.CoreV1().Services(testNamespace).Update(context.TODO(), currentSvc, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		ginkgo.By("Service should be announced by any available node")
		Eventually(func() string {
			return getAnnouncingNode(currentSvc)
		}, time.Minute, 2*time.Second).ShouldNot(BeEmpty())
		announcingNode := getAnnouncingNode(currentSvc)
		Expect(announcingNode).NotTo(BeEmpty())
	})
})
