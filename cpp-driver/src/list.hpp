/*
  Copyright (c) DataStax, Inc.

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

#ifndef DATASTAX_INTERNAL_LIST_HPP
#define DATASTAX_INTERNAL_LIST_HPP

#include "macros.hpp"

#include <stddef.h>

namespace datastax { namespace internal {

template <class T>
class List {
public:
  class Node {
  public:
    Node()
        : next_(NULL)
        , prev_(NULL) {}

  private:
    friend class List;
    Node* next_;
    Node* prev_;
  };

  template <class S>
  class Iterator {
  public:
    bool has_next() { return curr_ != end_; }

    S* next() {
      Node* temp = curr_;
      curr_ = curr_->next_;
      return static_cast<T*>(temp);
    }

  private:
    friend class List;
    Iterator(Node* begin, Node* end)
        : curr_(begin)
        , end_(end) {}

    Node* curr_;
    Node* end_;
  };

public:
  List()
      : size_(0) {
    data_.next_ = &data_;
    data_.prev_ = &data_;
  }

  void add_to_front(T* node);
  void add_to_back(T* node);
  void remove(T* node);

  T* front() {
    if (is_empty()) return NULL;
    return static_cast<T*>(data_.next_);
  }

  T* back() {
    if (is_empty()) return NULL;
    return static_cast<T*>(data_.prev_);
  }

  T* pop_front() {
    T* first = front();
    if (first != NULL) remove(first);
    return first;
  }

  size_t size() const { return size_; }
  bool is_empty() { return data_.next_ == &data_; }
  Iterator<T> iterator() { return Iterator<T>(data_.next_, &data_); }

private:
  void insert_before(Node* pos, Node* node);
  void insert_after(Node* pos, Node* node);

  Node data_;
  size_t size_;

private:
  DISALLOW_COPY_AND_ASSIGN(List);
};

template <class T>
void List<T>::add_to_front(T* node) {
  insert_after(&data_, node);
}

template <class T>
void List<T>::add_to_back(T* node) {
  insert_before(&data_, node);
}

template <class T>
void List<T>::remove(T* node) {
  size_--;
  node->prev_->next_ = node->next_;
  node->next_->prev_ = node->prev_;
  node->next_ = NULL;
  node->prev_ = NULL;
}

template <class T>
void List<T>::insert_after(Node* pos, Node* node) {
  size_++;
  pos->next_->prev_ = node;
  node->prev_ = pos;
  node->next_ = pos->next_;
  pos->next_ = node;
}

template <class T>
void List<T>::insert_before(Node* pos, Node* node) {
  size_++;
  pos->prev_->next_ = node;
  node->next_ = pos;
  node->prev_ = pos->prev_;
  pos->prev_ = node;
}

}} // namespace datastax::internal

#endif
