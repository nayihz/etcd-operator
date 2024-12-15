/*
Copyright 2024.

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
	"fmt"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	operatorv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

const controllerAgentName = "etcd-operator"

const (
	// SuccessSynced is used as part of the Event 'reason' when an EtcdCluster is synced
	SuccessSynced = "Synced"
	// ErrResourceExists is used as part of the Event 'reason' when an EtcdCluster fails
	// to sync due to a Statefulsets of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Statefulsets already existing
	MessageResourceExists = "Resource %q already exists and is not managed by EtcdCluster"
	// MessageResourceSynced is the message used for an Event fired when an EtcdCluster
	// is synced successfully
	MessageResourceSynced = "EtcdCluster synced successfully"
)

// EtcdClusterReconciler reconciles a EtcdCluster object
type EtcdClusterReconciler struct {
	client   client.Client
	Scheme   *runtime.Scheme
	recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/finalizers,verbs=update

func (r *EtcdClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var ec *operatorv1alpha1.EtcdCluster
	if err := r.client.Get(ctx, req.NamespacedName, ec); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	logger.Info("Syncing etcdcluster", "Spec", ec.Spec)

	var sts *appsv1.StatefulSet
	if err := r.client.Get(ctx, req.NamespacedName, sts); err != nil {
		if apierrors.IsNotFound(err) {
			if ec.Spec.Size > 0 {
				logger.Info("Creating statefulsets with 0 replica", "expectedSize", ec.Spec.Size)
				sts = newStatefulsets(ec, 0)
				err = r.client.Create(ctx, sts)
			} else {
				logger.Info("Skipping creating statefulsets due to the expected cluster size being 0")
				return ctrl.Result{}, nil
			}
		}
		// If an error occurs during Get/Create, we'll requeue the item so we can
		// attempt processing again later. This could have been caused by a
		// temporary network failure, or any other transient reason.
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	if !metav1.IsControlledBy(sts, ec) {
		msg := fmt.Sprintf(MessageResourceExists, sts.Name)
		r.recorder.Event(ec, corev1.EventTypeWarning, ErrResourceExists, msg)
		return ctrl.Result{}, fmt.Errorf("%s", msg)
	}

	memberListResp, healthInfos, err := healthCheck(ec, sts, logger)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("health check failed: %w", err)
	}

	memberCnt := 0
	if memberListResp != nil {
		memberCnt = len(memberListResp.Members)
	}
	replica := int(*sts.Spec.Replicas)

	if replica != memberCnt {
		// TODO: finish the logic later
		if replica < memberCnt {
			// a new added learner hasn't started yet

			// re-generate configuration for the new learner member;
			// increase statefulsets's replica by 1
		} else {
			// an already removed member hasn't stopped yet.

			// Decrease the statefulsets's replica by 1
		}
		// return
	}

	if memberCnt != len(healthInfos) {
		return ctrl.Result{}, fmt.Errorf("memberCnt (%d) isn't equal to healthy member count (%d)", memberCnt, len(healthInfos))
	}
	// There should be at most one learner, namely the last one
	if memberCnt > 0 && healthInfos[memberCnt-1].Status.IsLearner {
		logger = logger.WithValues("replica", replica, "expectedSize", ec.Spec.Size)

		learnerStatus := healthInfos[memberCnt-1].Status

		var leaderStatus *clientv3.StatusResponse
		for i := 0; i < memberCnt-1; i++ {
			status := healthInfos[i].Status
			if status.Leader == status.Header.MemberId {
				leaderStatus = status
				break
			}
		}

		if leaderStatus == nil {
			return ctrl.Result{}, fmt.Errorf("couldn't find leader, memberCnt: %d", memberCnt)
		}

		learnerID := healthInfos[memberCnt-1].Status.Header.MemberId
		if isLearnerReady(leaderStatus, learnerStatus) {
			logger.Info("Promoting the learner member", "learnerID", learnerID)
			eps := clientEndpointsFromStatefulsets(sts)
			eps = eps[:(len(eps) - 1)]
			return ctrl.Result{}, promoteLearner(eps, learnerID)
		}

		logger.Info("The learner member isn't ready to be promoted yet", "learnerID", learnerID)

		return ctrl.Result{}, nil
	}

	expectedSize := ec.Spec.Size
	if replica == expectedSize {
		// TODO: check version change, and perform upgrade if needed.
		return ctrl.Result{}, nil
	}

	var targetReplica int32
	if replica < expectedSize {
		// scale out
		targetReplica = int32(replica + 1)
		logger = logger.WithValues("targetReplica", targetReplica, "expectedSize", ec.Spec.Size)

		// TODO: check PV & PVC for the new member. If they already exist,
		// then it means they haven't been cleaned up yet when scaling in.

		_, peerURL := peerEndpointForOrdinalIndex(ec, replica)
		if replica > 0 {
			// if replica == 0, then it's the very first member, then
			// there is no need to add it as a learner; instead we can
			// start it as a voting member directly.
			eps := clientEndpointsFromStatefulsets(sts)
			logger.Info("[Scale out] adding a new learner member", "peerURLs", peerURL)
			if _, err := addMember(eps, []string{peerURL}, true); err != nil {
				return ctrl.Result{}, err
			}
		} else {
			logger.Info("[Scale out] Starting the very first voting member", "peerURLs", peerURL)
		}
	} else {
		// scale in
		targetReplica = int32(replica - 1)
		logger = logger.WithValues("targetReplica", targetReplica, "expectedSize", ec.Spec.Size)

		memberID := healthInfos[memberCnt-1].Status.Header.MemberId

		logger.Info("[Scale in] removing one member", "memberID", memberID)
		eps := clientEndpointsFromStatefulsets(sts)
		eps = eps[:targetReplica]
		if err := removeMember(eps, memberID); err != nil {
			return ctrl.Result{}, err
		}
	}

	logger.Info("Applying etcd cluster state")
	if err := r.applyEtcdClusterState(ec, int(targetReplica)); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Updating statefulsets")
	// sts, err = r.kubeClientset.AppsV1().StatefulSets(ec.Namespace).Update(context.TODO(), newStatefulsets(ec, targetReplica), metav1.UpdateOptions{})
	err = r.client.Update(ctx, newStatefulsets(ec, targetReplica))
	if err != nil {
		return ctrl.Result{}, err
	}

	r.recorder.Event(ec, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	return ctrl.Result{}, nil
}
func isLearnerReady(leaderStatus, learnerStatus *clientv3.StatusResponse) bool {
	leaderRev := leaderStatus.Header.Revision
	learnerRev := learnerStatus.Header.Revision

	learnerReadyPercent := float64(learnerRev) / float64(leaderRev)
	return learnerReadyPercent >= 0.9
}
func (c *EtcdClusterReconciler) applyEtcdClusterState(ec *operatorv1alpha1.EtcdCluster, replica int) error {
	cm := newEtcdClusterState(ec, replica)
	if err := c.client.Get(context.TODO(), types.NamespacedName{Namespace: ec.Namespace, Name: configMapNameForEtcdCluster(ec)}, cm); err != nil {
		if apierrors.IsNotFound(err) {
			cerr := c.client.Create(context.TODO(), cm)
			return cerr
		}
		return fmt.Errorf("cannot find ConfigMap for EtcdCluster %s: %w", ec.Name, err)
	}

	uerr := c.client.Update(context.TODO(), cm)
	return uerr
}
func newEtcdClusterState(ec *operatorv1alpha1.EtcdCluster, replica int) *corev1.ConfigMap {
	// We always add members one by one, so the state is always
	// "existing" if replica > 1.
	state := "new"
	if replica > 1 {
		state = "existing"
	}

	var initialCluster []string
	for i := 0; i < replica; i++ {
		name, peerURL := peerEndpointForOrdinalIndex(ec, i)
		initialCluster = append(initialCluster, fmt.Sprintf("%s=%s", name, peerURL))
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapNameForEtcdCluster(ec),
			Namespace: ec.Namespace,
		},
		Data: map[string]string{
			"ETCD_INITIAL_CLUSTER_STATE": state,
			"ETCD_INITIAL_CLUSTER":       strings.Join(initialCluster, ","),
		},
	}
}
func newStatefulsets(ec *operatorv1alpha1.EtcdCluster, replica int32) *appsv1.StatefulSet {
	labels := map[string]string{
		"app":        ec.Name,
		"controller": ec.Name,
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ec.Name,
			Namespace: ec.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(ec, operatorv1alpha1.GroupVersion.WithKind("EtcdCluster")),
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replica,
			ServiceName: ec.Name,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "etcd",
							Command: []string{"/usr/local/bin/etcd"},
							Args: []string{
								"--name=$(POD_NAME)",
								"--listen-peer-urls=http://0.0.0.0:2380",   //TODO: only listen on 127.0.0.1 and host IP
								"--listen-client-urls=http://0.0.0.0:2379", //TODO: only listen on 127.0.0.1 and host IP
								fmt.Sprintf("--initial-advertise-peer-urls=http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2380", ec.Name),
								fmt.Sprintf("--advertise-client-urls=http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2379", ec.Name),
							},
							Image: fmt.Sprintf("gcr.io/etcd-development/etcd:%s", ec.Spec.Version),
							Env: []corev1.EnvVar{
								{
									Name: "POD_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name: "POD_NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
							},
							EnvFrom: []corev1.EnvFromSource{
								{
									ConfigMapRef: &corev1.ConfigMapEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: configMapNameForEtcdCluster(ec),
										},
									},
								},
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "client",
									ContainerPort: 2379,
								},
								{
									Name:          "peer",
									ContainerPort: 2380,
								},
							},
						},
					},
				},
			},
		},
	}
}

func configMapNameForEtcdCluster(ec *operatorv1alpha1.EtcdCluster) string {
	return fmt.Sprintf("%s-state", ec.Name)
}
func clientEndpointsFromStatefulsets(sts *appsv1.StatefulSet) []string {
	var endpoints []string
	replica := int(*sts.Spec.Replicas)
	if replica > 0 {
		for i := 0; i < replica; i++ {
			endpoints = append(endpoints, clientEndpointForOrdinalIndex(sts, i))
		}
	}
	return endpoints
}

// healthCheck returns a memberList and an error.
// If any member (excluding not yet started or already removed member)
// is unhealthy, the error won't be nil.
func healthCheck(ec *operatorv1alpha1.EtcdCluster, sts *appsv1.StatefulSet, lg klog.Logger) (*clientv3.MemberListResponse, []epHealth, error) {
	replica := int(*sts.Spec.Replicas)
	if replica == 0 {
		return nil, nil, nil
	}

	endpoints := clientEndpointsFromStatefulsets(sts)

	memberlistResp, err := memberList(endpoints)
	if err != nil {
		return nil, nil, err
	}
	memberCnt := len(memberlistResp.Members)

	// Usually replica should be equal to memberCnt. If it isn't, then
	// it means previous reconcile loop somehow interrupted right after
	// adding (replica < memberCnt) or removing (replica > memberCnt)
	// a member from the cluster. In that case, we shouldn't run health
	// check on the not yet started or already removed member.
	cnt := min(replica, memberCnt)

	lg.Info("health checking", "replica", replica, "len(members)", memberCnt)
	endpoints = endpoints[:cnt]

	healthInfos, err := clusterHealth(endpoints)
	if err != nil {
		return memberlistResp, nil, err
	}

	for _, healthInfo := range healthInfos {
		if !healthInfo.Health {
			// TODO: also update metrics?
			return memberlistResp, healthInfos, fmt.Errorf(healthInfo.String())
		}
		lg.Info(healthInfo.String())
	}

	return memberlistResp, healthInfos, nil
}

func clientEndpointForOrdinalIndex(sts *appsv1.StatefulSet, index int) string {
	return fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2379",
		sts.Name, index, sts.Name, sts.Namespace)
}

func peerEndpointForOrdinalIndex(ec *operatorv1alpha1.EtcdCluster, index int) (string, string) {
	name := fmt.Sprintf("%s-%d", ec.Name, index)
	return name, fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2380",
		ec.Name, index, ec.Name, ec.Namespace)
}

func NewWorkloadReconciler(client client.Client, recorder record.EventRecorder) *EtcdClusterReconciler {
	return &EtcdClusterReconciler{
		client:   client,
		recorder: recorder,
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.EtcdCluster{}).
		Named("etcdcluster").
		Complete(r)
}
