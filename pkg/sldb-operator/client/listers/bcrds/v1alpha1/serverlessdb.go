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

// Code generated by lister-gen. DO NOT EDIT.

package v1alpha1

import (
	v1alpha1 "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// ServerlessDBLister helps list ServerlessDBs.
type ServerlessDBLister interface {
	// List lists all ServerlessDBs in the indexer.
	List(selector labels.Selector) (ret []*v1alpha1.ServerlessDB, err error)
	// ServerlessDBs returns an object that can list and get ServerlessDBs.
	ServerlessDBs(namespace string) ServerlessDBNamespaceLister
	ServerlessDBListerExpansion
}

// serverlessDBLister implements the ServerlessDBLister interface.
type serverlessDBLister struct {
	indexer cache.Indexer
}

// NewServerlessDBLister returns a new ServerlessDBLister.
func NewServerlessDBLister(indexer cache.Indexer) ServerlessDBLister {
	return &serverlessDBLister{indexer: indexer}
}

// List lists all ServerlessDBs in the indexer.
func (s *serverlessDBLister) List(selector labels.Selector) (ret []*v1alpha1.ServerlessDB, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.ServerlessDB))
	})
	return ret, err
}

// ServerlessDBs returns an object that can list and get ServerlessDBs.
func (s *serverlessDBLister) ServerlessDBs(namespace string) ServerlessDBNamespaceLister {
	return serverlessDBNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// ServerlessDBNamespaceLister helps list and get ServerlessDBs.
type ServerlessDBNamespaceLister interface {
	// List lists all ServerlessDBs in the indexer for a given namespace.
	List(selector labels.Selector) (ret []*v1alpha1.ServerlessDB, err error)
	// Get retrieves the ServerlessDB from the indexer for a given namespace and name.
	Get(name string) (*v1alpha1.ServerlessDB, error)
	ServerlessDBNamespaceListerExpansion
}

// serverlessDBNamespaceLister implements the ServerlessDBNamespaceLister
// interface.
type serverlessDBNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all ServerlessDBs in the indexer for a given namespace.
func (s serverlessDBNamespaceLister) List(selector labels.Selector) (ret []*v1alpha1.ServerlessDB, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.ServerlessDB))
	})
	return ret, err
}

// Get retrieves the ServerlessDB from the indexer for a given namespace and name.
func (s serverlessDBNamespaceLister) Get(name string) (*v1alpha1.ServerlessDB, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1alpha1.Resource("serverlessdb"), name)
	}
	return obj.(*v1alpha1.ServerlessDB), nil
}