#pragma once
#include <pthread.h>

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

/*
 * Here we implement a lock-free queue for buffering the
 * packets that come out from multiple filters.
 *
 * */

struct queue_root {
  struct queue_node *head;
  struct queue_node *tail;
  volatile unsigned int size;
};

struct queue_node {
  void *n;
  struct queue_node *next;
};

void init_queue(struct queue_root **root) {
  *root = (struct queue_root *)malloc(sizeof(struct queue_root));
  if (root == NULL) {
    printf("malloc failed");
    exit(1);
  }
  (*root)->head = (struct queue_node *)malloc(
      sizeof(struct queue_node)); /* Sentinel node */
  (*root)->tail = (*root)->head;
  (*root)->head->n = NULL;
  (*root)->head->next = NULL;
}

int queue_add(struct queue_root *root, void *val) {
  struct queue_node *n;
  struct queue_node *node =
      (struct queue_node *)malloc(sizeof(struct queue_node));
  node->n = val;
  node->next = NULL;
  while (1) {
    n = root->tail;
    if (__sync_bool_compare_and_swap(&(n->next), NULL, node)) {
      break;
    } else {
      __sync_bool_compare_and_swap(&(root->tail), n, n->next);
    }
  }
  __sync_bool_compare_and_swap(&(root->tail), n, node);

  return 1;
}

void *queue_get(struct queue_root *root) {
  struct queue_node *n;
  void *val;
  while (1) {
    n = root->head;
    if (n->next == NULL) {
      return NULL;
    }

    if (__sync_bool_compare_and_swap(&(root->head), n, n->next)) {
      break;
    }
  }
  val = (void *)n->next->n;
  free(n);
  return val;
}