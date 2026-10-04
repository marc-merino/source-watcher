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

package controller

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	gotkmeta "github.com/fluxcd/pkg/apis/meta"

	swapi "github.com/fluxcd/source-watcher/api/v2/v1beta1"
)

// namespaceConflict describes a managed namespace that is being adopted or
// taken over from another ArtifactGenerator.
type namespaceConflict struct {
	Namespace string `json:"namespace"`
	Owner     string `json:"owner,omitempty"`
	Adopted   bool   `json:"adopted"`
}

// namespaceClient returns the client used to manage cluster-scoped Namespace
// objects. When a ServiceAccount is configured for impersonation, it is used,
// otherwise the controller client is returned.
func (r *ArtifactGeneratorReconciler) namespaceClient(impersonated client.Client) client.Client {
	if impersonated != nil {
		return impersonated
	}
	return r.Client
}

// reconcileNamespaces creates or updates the namespaces explicitly targeted by
// the rendered output artifacts, returning their inventory entries and the
// namespaces that were adopted or taken over.
func (r *ArtifactGeneratorReconciler) reconcileNamespaces(ctx context.Context,
	obj *swapi.ArtifactGenerator,
	reqs []artifactRequest,
	impersonated client.Client) ([]swapi.InventoryEntry, []namespaceConflict, error) {
	kubeClient := r.namespaceClient(impersonated)
	seen := make(map[string]struct{})
	var refs []swapi.InventoryEntry
	var conflicts []namespaceConflict
	for _, req := range reqs {
		// Only namespaces explicitly targeted by .spec.artifacts[].namespace
		// are managed, the ArtifactGenerator namespace is never managed
		// implicitly through the default.
		name := req.Namespace
		if name == "" {
			continue
		}
		// Never manage the ArtifactGenerator namespace itself, even when
		// explicitly targeted, to avoid deleting the ArtifactGenerator and
		// its dependencies when pruning.
		if name == obj.Namespace {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}

		ref, conflict, err := r.reconcileNamespace(ctx, obj, name, kubeClient)
		if err != nil {
			return nil, nil, err
		}
		refs = append(refs, *ref)
		if conflict != nil {
			conflicts = append(conflicts, *conflict)
		}
	}
	return refs, conflicts, nil
}

// reconcileNamespace ensures the Namespace exists and is managed by the
// ArtifactGenerator, applying the common metadata and the controller labels.
// Existing namespaces that are not owned by another ArtifactGenerator are
// adopted with a warning event, while namespaces owned by another
// ArtifactGenerator are taken over.
func (r *ArtifactGeneratorReconciler) reconcileNamespace(ctx context.Context,
	obj *swapi.ArtifactGenerator,
	name string,
	kubeClient client.Client) (*swapi.InventoryEntry, *namespaceConflict, error) {
	log := ctrl.LoggerFrom(ctx)

	labels, annotations := r.namespaceMetadata(obj)
	digest := namespaceMetadataDigest(labels, annotations)

	// Detect adoption or ownership conflict before applying.
	var conflict *namespaceConflict
	changed := true
	existing := &corev1.Namespace{}
	err := kubeClient.Get(ctx, client.ObjectKey{Name: name}, existing)
	switch {
	case apierrors.IsNotFound(err):
		// The namespace will be created.
	case err != nil:
		return nil, nil, fmt.Errorf("failed to get Namespace: %w", err)
	default:
		changed = !namespaceMetadataMatches(existing, labels, annotations)
		owner := existing.Labels[swapi.ArtifactGeneratorLabel]
		switch {
		case owner == "":
			msg := fmt.Sprintf("Namespace %s is not managed by any ArtifactGenerator and is being adopted by %s/%s",
				name, obj.Namespace, obj.Name)
			log.Info("Adopting unmanaged namespace", "namespace", name)
			r.Event(existing, corev1.EventTypeWarning, swapi.NamespaceAdoptedReason, msg)
			conflict = &namespaceConflict{Namespace: name, Adopted: true}
		case owner != string(obj.GetUID()):
			msg := fmt.Sprintf("Namespace %s is managed by ArtifactGenerator %s and is being taken over by %s/%s",
				name, owner, obj.Namespace, obj.Name)
			log.Info("Taking over managed namespace", "namespace", name, "owner", owner)
			r.Event(existing, corev1.EventTypeWarning, swapi.OwnershipConflictReason, msg)
			conflict = &namespaceConflict{Namespace: name, Owner: owner}
		}
	}

	ns := &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       swapi.NamespaceKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      labels,
			Annotations: annotations,
		},
	}

	forceApply := true
	if err := kubeClient.Patch(ctx, ns, client.Apply, &client.PatchOptions{
		FieldManager: r.ControllerName,
		Force:        &forceApply,
	}); err != nil {
		return nil, nil, fmt.Errorf("failed to apply Namespace: %w", err)
	}

	if changed {
		msg := fmt.Sprintf("Namespace %s reconciled", name)
		log.Info(msg)
		r.Event(obj, eventv1.EventTypeTrace, gotkmeta.ReadyCondition, msg)
	} else {
		log.Info("Namespace is up to date", "namespace", name)
	}

	return &swapi.InventoryEntry{
		Kind:   swapi.NamespaceKind,
		Name:   name,
		Digest: digest,
	}, conflict, nil
}

// namespaceMetadata returns the labels and annotations to apply to managed
// namespaces. The common metadata is applied, with the controller labels
// taking precedence.
func (r *ArtifactGeneratorReconciler) namespaceMetadata(obj *swapi.ArtifactGenerator) (map[string]string, map[string]string) {
	labels := make(map[string]string)
	var annotations map[string]string
	if cm := obj.Spec.CommonMetadata; cm != nil {
		maps.Copy(labels, cm.Labels)
		if len(cm.Annotations) > 0 {
			annotations = make(map[string]string)
			maps.Copy(annotations, cm.Annotations)
		}
	}

	labels["app.kubernetes.io/managed-by"] = r.ControllerName
	labels[swapi.ArtifactGeneratorLabel] = string(obj.GetUID())
	return labels, annotations
}

// namespaceMetadataMatches returns true when the namespace carries all the
// expected labels and annotations.
func namespaceMetadataMatches(ns *corev1.Namespace, labels, annotations map[string]string) bool {
	for key, value := range labels {
		if ns.Labels[key] != value {
			return false
		}
	}
	for key, value := range annotations {
		if ns.Annotations[key] != value {
			return false
		}
	}
	return true
}

// namespaceMetadataDigest computes a deterministic digest of the metadata
// applied to a managed namespace.
func namespaceMetadataDigest(labels, annotations map[string]string) string {
	parts := make([]string, 0, len(labels)+len(annotations))
	for key, value := range labels {
		parts = append(parts, fmt.Sprintf("label:%s=%s", key, value))
	}
	for key, value := range annotations {
		parts = append(parts, fmt.Sprintf("annotation:%s=%s", key, value))
	}
	sort.Strings(parts)
	digest := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return fmt.Sprintf("sha256:%x", digest)
}

// detectNamespacesDrift checks if the managed namespaces recorded in the
// inventory still exist, are owned by the ArtifactGenerator and carry the
// expected common metadata.
func (r *ArtifactGeneratorReconciler) detectNamespacesDrift(ctx context.Context,
	obj *swapi.ArtifactGenerator,
	impersonated client.Client) (bool, error) {
	if !obj.ManagesNamespaces() {
		return false, nil
	}

	expectedLabels, expectedAnnotations := r.namespaceMetadata(obj)
	kubeClient := r.namespaceClient(impersonated)
	for _, ref := range obj.Status.Inventory {
		if ref.Kind != swapi.NamespaceKind {
			continue
		}
		ns := &corev1.Namespace{}
		if err := kubeClient.Get(ctx, client.ObjectKey{Name: ref.Name}, ns); err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		if !namespaceMetadataMatches(ns, expectedLabels, expectedAnnotations) {
			return true, nil
		}
	}
	return false, nil
}

// deleteNamespace deletes a managed namespace. It returns an error when the
// deletion was not confirmed by the API server.
func (r *ArtifactGeneratorReconciler) deleteNamespace(ctx context.Context,
	name string,
	impersonated client.Client) error {
	log := ctrl.LoggerFrom(ctx)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := r.namespaceClient(impersonated).Delete(ctx, ns); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	log.Info("Namespace deleted from cluster", "namespace", name)
	return nil
}
