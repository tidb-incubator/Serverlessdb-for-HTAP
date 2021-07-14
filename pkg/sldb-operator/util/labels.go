package util

import (
	"fmt"
	pclabel "github.com/pingcap/tidb-operator/pkg/label"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"strings"
)

const (
	// InstanceLabelKey is Kubernetes recommended label key, it represents a unique name identifying the instance of an application
	// It's set by helm when installing a release
	InstanceLabelKey string = "app.kubernetes.io/instance"

	InstanceAnnotationKey string = "bcrds.cmss.com/instance"
	BcRdsInstanceLabelKey        = InstanceAnnotationKey

	// RestartAtAnnotationKey represents restart flag
	RestartAtAnnotationKey string = "bcrds.cmss.com/restartAt"

	// RestartAtAnnotationKey represents restart flag
	PingcapRestartAtAnnotationKey string = "tidb.pingcap.com/restartedAt"

	// SLDBOperator is ManagedByLabelKey label value
	SLDBOperator string = "sldb-operator"
)

// Label is the label field in metadata
type Label map[string]string

// New initialize a new Label for components of tidb cluster
func New() Label {
	return Label{
		pclabel.ManagedByLabelKey: SLDBOperator,
	}
}

// Instance adds instance kv pair to label
func (l Label) Instance(name string) Label {
	l[InstanceLabelKey] = name
	return l
}

// Instance adds instance kv pair to label
func (l Label) BcRdsInstance(name string) Label {
	l[BcRdsInstanceLabelKey] = name
	return l
}

// Component adds component kv pair to label
func (l Label) Component(name string) Label {
	l[pclabel.ComponentLabelKey] = name
	return l
}

// Selector gets labels.Selector from label
func (l Label) Selector() (labels.Selector, error) {
	return metav1.LabelSelectorAsSelector(l.LabelSelector())
}

// LabelSelector gets LabelSelector from label
func (l Label) LabelSelector() *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: l}
}

// Labels converts label to map[string]string
func (l Label) Labels() map[string]string {
	return l
}

// Copy copy the value of label to avoid pointer copy
func (l Label) Copy() Label {
	copyLabel := make(Label)
	for k, v := range l {
		copyLabel[k] = v
	}
	return copyLabel
}

// String converts label to a string
func (l Label) String() string {
	var arr []string

	for k, v := range l {
		arr = append(arr, fmt.Sprintf("%s=%s", k, v))
	}

	return strings.Join(arr, ",")
}

// LabelsForServerlessDB returns the labels for selecting the resources
// belonging to the given CR name.
func LabelsForServerlessDB(name string) map[string]string {
	label := New().Instance(name).BcRdsInstance(name)
	return label.Labels()
}

// LabelsProxyForServerlessDB returns the labels for selecting the resources
// belonging to the given CR name.
func LabelsComponentForServerlessDB(name, component string) map[string]string {
	label := New().Instance(name).BcRdsInstance(name).Component(component)
	return label.Labels()
}

func LabelEq(key string, val string) *labels.Requirement {
	r, _ := labels.NewRequirement(key, selection.In, []string{val})
	return r
}
