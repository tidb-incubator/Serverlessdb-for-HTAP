/*
Copyright CMSS Chinamobile Authors.

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

// Code generated by client-gen. DO NOT EDIT.

package v1alpha1

import (
	v1alpha1 "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	scheme "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client/clientset/versioned/scheme"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// ServerlessDBsGetter has a method to return a ServerlessDBInterface.
// A group's client should implement this interface.
type ServerlessDBsGetter interface {
	ServerlessDBs(namespace string) ServerlessDBInterface
}

// ServerlessDBInterface has methods to work with ServerlessDB resources.
type ServerlessDBInterface interface {
	Create(*v1alpha1.ServerlessDB) (*v1alpha1.ServerlessDB, error)
	Update(*v1alpha1.ServerlessDB) (*v1alpha1.ServerlessDB, error)
	UpdateStatus(*v1alpha1.ServerlessDB) (*v1alpha1.ServerlessDB, error)
	Delete(name string, options *v1.DeleteOptions) error
	DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error
	Get(name string, options v1.GetOptions) (*v1alpha1.ServerlessDB, error)
	List(opts v1.ListOptions) (*v1alpha1.ServerlessDBList, error)
	Watch(opts v1.ListOptions) (watch.Interface, error)
	Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.ServerlessDB, err error)
	ServerlessDBExpansion
}

// serverlessDBs implements ServerlessDBInterface
type serverlessDBs struct {
	client rest.Interface
	ns     string
}

// newServerlessDBs returns a ServerlessDBs
func newServerlessDBs(c *BcrdsV1alpha1Client, namespace string) *serverlessDBs {
	return &serverlessDBs{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the serverlessDB, and returns the corresponding serverlessDB object, and an error if there is any.
func (c *serverlessDBs) Get(name string, options v1.GetOptions) (result *v1alpha1.ServerlessDB, err error) {
	result = &v1alpha1.ServerlessDB{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("serverlessdbs").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do().
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of ServerlessDBs that match those selectors.
func (c *serverlessDBs) List(opts v1.ListOptions) (result *v1alpha1.ServerlessDBList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1alpha1.ServerlessDBList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("serverlessdbs").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do().
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested serverlessDBs.
func (c *serverlessDBs) Watch(opts v1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("serverlessdbs").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch()
}

// Create takes the representation of a serverlessDB and creates it.  Returns the server's representation of the serverlessDB, and an error, if there is any.
func (c *serverlessDBs) Create(serverlessDB *v1alpha1.ServerlessDB) (result *v1alpha1.ServerlessDB, err error) {
	result = &v1alpha1.ServerlessDB{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("serverlessdbs").
		Body(serverlessDB).
		Do().
		Into(result)
	return
}

// Update takes the representation of a serverlessDB and updates it. Returns the server's representation of the serverlessDB, and an error, if there is any.
func (c *serverlessDBs) Update(serverlessDB *v1alpha1.ServerlessDB) (result *v1alpha1.ServerlessDB, err error) {
	result = &v1alpha1.ServerlessDB{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("serverlessdbs").
		Name(serverlessDB.Name).
		Body(serverlessDB).
		Do().
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().

func (c *serverlessDBs) UpdateStatus(serverlessDB *v1alpha1.ServerlessDB) (result *v1alpha1.ServerlessDB, err error) {
	result = &v1alpha1.ServerlessDB{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("serverlessdbs").
		Name(serverlessDB.Name).
		SubResource("status").
		Body(serverlessDB).
		Do().
		Into(result)
	return
}

// Delete takes name of the serverlessDB and deletes it. Returns an error if one occurs.
func (c *serverlessDBs) Delete(name string, options *v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("serverlessdbs").
		Name(name).
		Body(options).
		Do().
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *serverlessDBs) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	var timeout time.Duration
	if listOptions.TimeoutSeconds != nil {
		timeout = time.Duration(*listOptions.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("serverlessdbs").
		VersionedParams(&listOptions, scheme.ParameterCodec).
		Timeout(timeout).
		Body(options).
		Do().
		Error()
}

// Patch applies the patch and returns the patched serverlessDB.
func (c *serverlessDBs) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.ServerlessDB, err error) {
	result = &v1alpha1.ServerlessDB{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("serverlessdbs").
		SubResource(subresources...).
		Name(name).
		Body(data).
		Do().
		Into(result)
	return
}
