/*
Copyright 2026 The Flux authors

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

package v1beta1_test

import (
	"testing"

	"github.com/fluxcd/source-watcher/api/v2/v1beta1"
)

func TestArtifactGeneratorGetArtifactNamespace(t *testing.T) {
	obj := &v1beta1.ArtifactGenerator{}
	obj.Namespace = "generator-ns"

	if got := obj.GetArtifactNamespace(&v1beta1.OutputArtifact{}); got != "generator-ns" {
		t.Errorf("GetArtifactNamespace() = %q, want %q", got, "generator-ns")
	}

	if got := obj.GetArtifactNamespace(&v1beta1.OutputArtifact{Namespace: "target-ns"}); got != "target-ns" {
		t.Errorf("GetArtifactNamespace() = %q, want %q", got, "target-ns")
	}
}

func TestArtifactGeneratorNamespaces(t *testing.T) {
	obj := &v1beta1.ArtifactGenerator{}

	if obj.ManagesNamespaces() {
		t.Error("ManagesNamespaces() = true, want false when namespaces is not set")
	}
	if !obj.NamespacePrune() {
		t.Error("NamespacePrune() = false, want true when namespaces is not set")
	}

	obj.Spec.Namespaces = &v1beta1.Namespaces{Strategy: v1beta1.NamespaceStrategyUnmanaged}
	if obj.ManagesNamespaces() {
		t.Error("ManagesNamespaces() = true, want false for Unmanaged")
	}
	if !obj.NamespacePrune() {
		t.Error("NamespacePrune() = false, want true when prune is not set")
	}

	prune := false
	obj.Spec.Namespaces = &v1beta1.Namespaces{
		Strategy: v1beta1.NamespaceStrategyManaged,
		Prune:    &prune,
	}
	if !obj.ManagesNamespaces() {
		t.Error("ManagesNamespaces() = false, want true for Managed")
	}
	if obj.NamespacePrune() {
		t.Error("NamespacePrune() = true, want false when prune is false")
	}
}

func TestArtifactGeneratorInventoryHelpers(t *testing.T) {
	obj := &v1beta1.ArtifactGenerator{
		Status: v1beta1.ArtifactGeneratorStatus{
			Inventory: []v1beta1.InventoryEntry{
				{
					Kind:      "ExternalArtifact",
					Name:      "app",
					Namespace: "tenant",
					Digest:    "sha256:abc",
					Filename:  "app.tar.gz",
				},
				{
					Kind:   v1beta1.NamespaceKind,
					Name:   "tenant",
					Digest: "sha256:def",
				},
			},
		},
	}

	if !obj.HasArtifactInInventory("app", "tenant", "sha256:abc") {
		t.Error("HasArtifactInInventory() = false, want true")
	}
	if obj.HasArtifactInInventory("tenant", "", "") {
		t.Error("HasArtifactInInventory() matched a namespace entry")
	}
	if !obj.HasNamespaceInInventory("tenant", "sha256:def") {
		t.Error("HasNamespaceInInventory() = false, want true")
	}
	if obj.HasNamespaceInInventory("tenant", "sha256:other") {
		t.Error("HasNamespaceInInventory() = true for a different digest")
	}
	if obj.HasNamespaceInInventory("app", "sha256:def") {
		t.Error("HasNamespaceInInventory() = true for an artifact entry")
	}
}
