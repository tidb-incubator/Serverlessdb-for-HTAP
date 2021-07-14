package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ClusterPhase is the current state of tidb
type ClusterPhase string

const (
	// Creating represents normal state of cluster.
	PhaseCreating ClusterPhase = "Creating"
	// Indicates cluster is being recovering or going to recover.
	PhaseRecovering ClusterPhase = "Recovering"
	// PhaseAvailable indicates that the cluster status is normal and available
	PhaseAvailable ClusterPhase = "Available"
	// Unavailable represents the scaling state of cluster.
	PhaseUnavailable ClusterPhase = "Unavailable"
	// PhaseFailed indicates that the cluster completely failed, unable to recover
	PhaseFailed ClusterPhase = "Failed"
)

type MaxValue struct {
	Metric    `json:",inline"`
	Component Storage `json:"component,omitempty"`
}

type StorageType string

const (
	S3    StorageType = "S3"
	CLOUD StorageType = "cloud"
	LOCAL StorageType = "local"
)

type Storage struct {
	// StorageSize is the request storage size for backup job
	StorageType StorageType `json:"type,omitempty"`
	// The storageClassName of the persistent volume for Backup data storage.
	// Defaults to Kubernetes default storage class.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
	// StorageSize is the request storage size
	StorageSize string `json:"storageSize,omitempty"`
}

type PeriodType string

const (
	CUSTOMIZE PeriodType = ""
	DAY       PeriodType = "day"
	WEEK      PeriodType = "week"
	MONTH     PeriodType = "month"
)

type Rule struct {
	StartTime *metav1.Time `json:"startTime,omitempty"`
	EndTime   *metav1.Time `json:"endTime,omitempty"`
	Metric    Metric       `json:"metric,omitempty"`
	Period    PeriodType   `json:"period,omitempty"`
}

type Metric struct {
	Performance Performance `json:"performance,omitempty"`
	HashRate    string      `json:"hashRate,omitempty"`
}

type Performance struct {
	Delay          string `json:"delay,omitempty"`
	MaxTPMC        string `json:"maxTPMC,omitempty"`
	MaxConnections string `json:"maxConnections,omitempty"`
}

type Proxy struct {
	// Defaults to "0".
	// +optional
	// +kubebuilder:default=0
	SilentPeriod string `json:"silentPeriod,omitempty"`
}

// ServerlessDBSpec defines the desired state of ServerlessDB
// +k8s:openapi-gen=true
type ServerlessDBSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
	// Add custom validation using kubebuilder tags: https://book-v1.book.kubebuilder.io/beyond_basics/generating_crd.html

	// Indicates that the severlessdb is paused and will not be processed by
	// the controller.
	// +optional
	Paused bool `json:"paused,omitempty"`

	// Prevent external connections to tidb server
	// +optional
	Freeze bool `json:"freeze,omitempty"`

	// +optional
	// +kubebuilder:default=IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy"`

	Proxy    Proxy    `json:"proxy,omitempty"`
	MaxValue MaxValue `json:"maxValue,omitempty"`

	// Indicates that the tiflash cluster is used and will not be processed by
	// the controller.
	// +optional
	UseTiFlash bool   `json:"useTiFlash,omitempty"`
	ScaleOut   string `json:"scaleOut,omitempty"`
	ScaleIn    string `json:"scaleIn,omitempty"`

	RateOfScaleDown string `json:"rateOfScaleDown,omitempty"`
	TimeToScaleDown string `json:"timeToScaleDown,omitempty"`

	NormalResource corev1.ResourceRequirements `json:"normalResource,omitempty"`
	MinResource    corev1.ResourceRequirements `json:"minResource,omitempty"`

	Rule map[string]Rule `json:"rule,omitempty"`
}

// ConditionType represents a cluster condition value.
type ConditionType string

const (
	// Ready indicates that the cluster is ready or not.
	// This is defined as:
	// - All Components reach usable level
	// todo: add condition type
	Ready ConditionType = "Ready"

	// TiDBInitial: waiting tidb initial successful,
	// status.phase will still be Creating if condition exist TiDBInitial type
	TiDBInitial  ConditionType = "TiDBInitial"
	TiDBRestart  ConditionType = "TiDBRestart"
	TiDBSilence  ConditionType = "TiDBSilence"
	TiDBReadonly ConditionType = "TiDBReadonly"
)

// ServerlessDBCondition describes the state of a cluster at a certain point.
type ServerlessDBCondition struct {
	// Type of the condition.
	Type ConditionType `json:"type"`
	// Status of the condition, one of True, False, Unknown.
	Status corev1.ConditionStatus `json:"status"`
	// The last time this condition was updated.
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
	// Last time the condition transitioned from one status to another.
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
	// The reason for the condition's last transition.
	// +optional
	Reason string `json:"reason,omitempty"`
	// A human readable message indicating details about the transition.
	// +optional
	Message string `json:"message,omitempty"`
}

type PortName string

const (
	Prometheus PortName = "Prometheus"
	TiDBServer PortName = "TiDBServer"
)

type StorageUse string

const (
	TiKVSingleStoreMax  StorageUse = "TiKVSingleStoreMax"
	TiKVTotalStoreUsage StorageUse = "TiKVTotalStoreUsage"
)

// ServerlessDBStatus defines the observed state of ServerlessDB
type ServerlessDBStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
	// Add custom validation using kubebuilder tags: https://book-v1.book.kubebuilder.io/beyond_basics/generating_crd.html
	// Rule is the Currently triggered rule
	Rule []string `json:"rule,omitempty"`

	// Represents the latest phase
	Phase ClusterPhase `json:"phase,omitempty"`

	// Node port for sldb
	NodePort map[PortName]int32 `json:"nodePort,omitempty"`

	// storage usage
	StorageUsage map[StorageUse]resource.Quantity `json:"storageUsage,omitempty"`

	// Represents the latest available observations of a cluster's state.
	// +optional
	Conditions []ServerlessDBCondition `json:"conditions,omitempty"`

	// LastBackupTime represents the last time the backup was successfully created.
	// +optional
	LastBackupTime *metav1.Time `json:"lastBackupTime,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ServerlessDB is the Schema for the serverlessdbs API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=serverlessdbs,scope=Namespaced
type ServerlessDB struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ServerlessDBSpec   `json:"spec"`
	Status ServerlessDBStatus `json:"status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ServerlessDBList contains a list of ServerlessDB
type ServerlessDBList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []ServerlessDB `json:"items"`
}
