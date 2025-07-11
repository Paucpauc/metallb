// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"crypto/sha256"
	"maps"
	"net"
	"sort"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"

	"go.universe.tf/metallb/internal/config"
	"go.universe.tf/metallb/internal/k8s/epslices"
	k8snodes "go.universe.tf/metallb/internal/k8s/nodes"
	"go.universe.tf/metallb/internal/layer2"
)

const (
	primaryNodeAnnotation = "metallb.io" + "/" + "primary-node"
)

type layer2Controller struct {
	announcer       *layer2.Announce
	myNode          string
	ignoreExcludeLB bool
	sList           SpeakerList
	onStatusChange  func(types.NamespacedName)
}

func (c *layer2Controller) SetConfig(log.Logger, *config.Config) error {
	return nil
}

// nodesWithEndpoint returns all nodes that have at least one fully ready
// endpoint on them and that have a speaker according to the speaker map param.
func nodesWithEndpoint(eps []discovery.EndpointSlice, speakers map[string]bool) []string {
	usable := map[string]bool{}
	for _, slice := range eps {
		for _, ep := range slice.Endpoints {
			if !epslices.EndpointCanServe(ep.Conditions) {
				continue
			}
			if ep.NodeName == nil {
				continue
			}
			nodeName := *ep.NodeName
			if hasSpeaker := speakers[nodeName]; !hasSpeaker {
				continue
			}
			if _, ok := usable[nodeName]; !ok {
				usable[nodeName] = true
			}
		}
	}

	var ret []string
	for node, ok := range usable {
		if ok {
			ret = append(ret, node)
		}
	}

	return ret
}

func (c *layer2Controller) ShouldAnnounce(l log.Logger, name string, toAnnounce []net.IP, pool *config.Pool, svc *v1.Service, eps []discovery.EndpointSlice, nodes map[string]*v1.Node) string {
	if !activeEndpointExists(eps) { // no active endpoints, just return
		level.Debug(l).Log("event", "shouldannounce", "protocol", "l2", "message", "failed no active endpoints", "service", name)
		return "notOwner"
	}

	if !poolMatchesNodeL2(pool, c.myNode) {
		level.Debug(l).Log("event", "skipping should announce l2", "service", name, "reason", "pool not matching my node")
		return "notOwner"
	}

	speakerMap := c.speakersForPool(l, name, pool, nodes)
	availableNodes := nodesWithActiveSpeakers(speakerMap)
	if svc.Spec.ExternalTrafficPolicy == v1.ServiceExternalTrafficPolicyTypeLocal {
		availableNodes = nodesWithEndpoint(eps, speakerMap)
	}

	if len(availableNodes) == 0 {
		level.Debug(l).Log("event", "skipping should announce l2", "service", name, "reason", "no available nodes")
		return "notOwner"
	}

	level.Debug(l).Log("event", "shouldannounce", "protocol", "l2", "nodes", availableNodes, "service", name)

	// Using the first IP should work for both single and dual stack.
	ipString := toAnnounce[0].String()

	primaryNode, _ := svc.Annotations[primaryNodeAnnotation]
	level.Debug(l).Log("event", "shouldannounce", "protocol", "l2", "primaryNode", primaryNode, "service", name)

	// Sort the slice by the hash of node + load balancer ips. This
	// produces an ordering of ready nodes that is unique to all the services
	// with the same ip.
	sort.Slice(availableNodes, func(i, j int) bool {
		if availableNodes[i] == primaryNode {
			return true
		}
		if availableNodes[j] == primaryNode {
			return false
		}
		hi := sha256.Sum256([]byte(availableNodes[i] + "#" + ipString))
		hj := sha256.Sum256([]byte(availableNodes[j] + "#" + ipString))

		return bytes.Compare(hi[:], hj[:]) < 0
	})

	// Are we first in the list? If so, we win and should announce.
	if len(availableNodes) > 0 && availableNodes[0] == c.myNode {
		return ""
	}

	// Either not eligible, or lost the election entirely.
	return "notOwner"
}

func (c *layer2Controller) SetBalancer(l log.Logger, name string, lbIPs []net.IP, pool *config.Pool, client service, svc *v1.Service) error {
	ifs := c.announcer.GetInterfaces()
	updateStatus := false
	for _, lbIP := range lbIPs {
		ipAdv := ipAdvertisementFor(lbIP, c.myNode, pool.L2Advertisements)
		if !ipAdv.MatchInterfaces(ifs...) {
			level.Warn(l).Log("op", "SetBalancer", "protocol", "layer2", "service", name, "IPAdvertisement", ipAdv,
				"localIfs", ifs, "msg", "the specified interfaces used to announce LB IP don't exist")
			client.Errorf(svc, "announceFailed", "the interfaces specified by LB IP %q doesn't exist in assigned node %q with protocol %q", lbIP.String(), c.myNode, config.Layer2)
			continue
		}
		c.announcer.SetBalancer(name, ipAdv)
		updateStatus = true
	}
	if updateStatus {
		c.onStatusChange(types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace})
	}
	return nil
}

func (c *layer2Controller) DeleteBalancer(l log.Logger, name, reason string) error {
	if !c.announcer.AnnounceName(name) {
		return nil
	}
	c.announcer.DeleteBalancer(name)

	svcNamespace, svcName, err := cache.SplitMetaNamespaceKey(name)
	if err != nil {
		level.Warn(l).Log("op", "DeleteBalancer", "protocol", "layer2", "service", name, "msg", "failed to split key", "err", err)
		return err
	}
	c.onStatusChange(types.NamespacedName{Name: svcName, Namespace: svcNamespace})
	return nil
}

func (c *layer2Controller) SetNode(l log.Logger, n *v1.Node) error {
	if c.myNode != n.Name {
		return nil
	}
	c.sList.Rejoin()
	return nil
}

func (c *layer2Controller) SetEventCallback(callback func(interface{})) {
	// Do nothing
}

func ipAdvertisementFor(ip net.IP, localNode string, l2Advertisements []*config.L2Advertisement) layer2.IPAdvertisement {
	ifs := sets.Set[string]{}
	for _, l2 := range l2Advertisements {
		if matchNode := l2.Nodes[localNode]; !matchNode {
			continue
		}
		if l2.AllInterfaces {
			return layer2.NewIPAdvertisement(ip, true, sets.Set[string]{})
		}
		ifs = ifs.Insert(l2.Interfaces...)
	}
	return layer2.NewIPAdvertisement(ip, false, ifs)
}

// nodesWithActiveSpeakers returns the list of nodes with active speakers.
func nodesWithActiveSpeakers(speakers map[string]bool) []string {
	var ret []string
	for node := range speakers {
		ret = append(ret, node)
	}
	return ret
}

// activeEndpointExists returns true if at least one endpoint is active.
func activeEndpointExists(eps []discovery.EndpointSlice) bool {
	for _, slice := range eps {
		for _, ep := range slice.Endpoints {
			if !epslices.EndpointCanServe(ep.Conditions) {
				continue
			}
			return true
		}
	}
	return false
}

func poolMatchesNodeL2(pool *config.Pool, node string) bool {
	for _, adv := range pool.L2Advertisements {
		if adv.Nodes[node] {
			return true
		}
	}
	return false
}

func (c *layer2Controller) speakersForPool(l log.Logger, name string, pool *config.Pool, nodes map[string]*v1.Node) map[string]bool {
	sl := c.sList.UsableSpeakers()
	eligibleNodes := maps.Keys(sl.Nodes)
	if sl.Disabled {
		// when memberlist is disabled we consider all nodes eligible under the assumption that
		// there is a speaker pod running on all nodes.
		eligibleNodes = maps.Keys(nodes)
	}
	res := map[string]bool{}
	for s := range eligibleNodes {
		if k8snodes.IsNetworkUnavailable(nodes[s]) {
			level.Debug(l).Log("event", "skipping should announce l2", "service", name, "reason", "speaker's node has NodeNetworkUnavailable condition")
			continue
		}

		if !c.ignoreExcludeLB && k8snodes.IsNodeExcludedFromBalancers(nodes[s]) {
			level.Debug(l).Log("event", "skipping should announce l2", "service", name, "reason", "speaker's node has labeled 'node.kubernetes.io/exclude-from-external-load-balancers'")
			continue
		}

		if poolMatchesNodeL2(pool, s) {
			res[s] = true
		}
	}
	return res
}
