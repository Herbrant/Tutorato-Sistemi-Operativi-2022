#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#define IN1 1
#define IN2 2
#define BUFFER_SIZE 1024

typedef struct {
  long type;
  char key[BUFFER_SIZE];
  char id;
  char done;
} query_msg;

typedef struct {
  char key[BUFFER_SIZE];
  int value;
} entry;

typedef struct {
  long type;
  char id;
  char done;
  entry e;
} out_msg;

typedef struct {
  entry *e;
  struct node *next;
} node;

typedef node *list;

list insert(list l, entry *e) {
  node *n = malloc(sizeof(node));
  n->e = e;
  n->next = NULL;

  if (l == NULL)
    l = n;
  else {
    n->next = (struct node *)l;
    l = n;
  }

  return l;
}

entry *search(list l, char *key) {
  node *ptr = l;

  while (ptr != NULL) {
    if (!strcmp(ptr->e->key, key))
      return ptr->e;

    ptr = (node *)ptr->next;
  }

  return NULL;
}

void print(list l) {
  node *ptr = l;

  while (ptr != NULL) {
    printf("Entry -> key: %s, value: %d\n", ptr->e->key, ptr->e->value);
    ptr = (node *)ptr->next;
  }
}

void destroy(list l) {
  node *ptr = l;
  node *tmp;

  while (ptr != NULL) {
    tmp = ptr;
    ptr = (node *)ptr->next;
    free(tmp->e);
    free(tmp);
  }
}

void in_child(char id, int queue, char *path) {
  FILE *f;
  query_msg msg;
  msg.type = 1;
  msg.done = 0;
  msg.id = id;
  unsigned counter = 0;

  if ((f = fopen(path, "r")) == NULL) {
    perror("fopen");
    exit(1);
  }

  while (fgets(msg.key, BUFFER_SIZE, f)) {
    counter++;
    if (msg.key[strlen(msg.key) - 1] == '\n')
      msg.key[strlen(msg.key) - 1] = '\0';

    if (msgsnd(queue, &msg, sizeof(query_msg) - sizeof(long), 0) == -1) {
      perror("msgsnd");
      exit(1);
    }

    printf("IN%d: inviata query n.%d '%s'\n", id, counter, msg.key);
  }

  msg.done = 1;
  if (msgsnd(queue, &msg, sizeof(query_msg) - sizeof(long), 0) == -1) {
    perror("msgsnd");
    exit(1);
  }

  fclose(f);
  exit(0);
}

entry *create_entry(char *data) {
  entry *e = malloc(sizeof(entry));
  char *key;
  char *value;

  if (data[strlen(data) - 1] == '\n')
    data[strlen(data) - 1] = '\0';

  if ((key = strtok(data, ":")) != NULL) {
    if ((value = strtok(NULL, ":")) != NULL) {
      strncpy(e->key, key, BUFFER_SIZE);
      e->value = atoi(value);
      return e;
    }
  }

  free(e);
  return NULL;
}

list load_database(char *path) {
  list l = NULL;
  FILE *f;
  entry *e;
  char buffer[BUFFER_SIZE];
  unsigned counter = 0;

  if ((f = fopen(path, "r")) == NULL) {
    perror("fopen");
    exit(1);
  }

  while (fgets(buffer, BUFFER_SIZE, f)) {
    e = create_entry(buffer);
    if (e != NULL) {
      l = insert(l, e);
      counter++;
    }
  }

  printf("DB: letti n.%d record da file\n", counter);
  fclose(f);
  return l;
}

void db_child(int in_queue, int out_queue, char *path) {
  query_msg q_msg;
  out_msg o_msg;
  o_msg.type = 1;
  o_msg.done = 0;
  char done_counter = 0;
  list l = load_database(path);
  entry *e;

  while (1) {
    if (msgrcv(in_queue, &q_msg, sizeof(query_msg) - sizeof(long), 0, 0) ==
        -1) {
      perror("msgrcv");
      exit(1);
    }

    if (q_msg.done) {
      done_counter++;
      if (done_counter < 2)
        continue;
      else
        break;
    }

    e = search(l, q_msg.key);

    if (e != NULL) {
      o_msg.e = *e;
      o_msg.id = q_msg.id;
      if (msgsnd(out_queue, &o_msg, sizeof(out_msg) - sizeof(long), 0) == -1) {
        perror("msgsnd");
        exit(1);
      }

      printf("DB: query '%s' da IN%d trovata con valore %d\n", q_msg.key,
             q_msg.id, e->value);

    } else
      printf("DB: query '%s' da IN%d non trovata\n", q_msg.key, q_msg.id);
  }

  o_msg.done = 1;

  if (msgsnd(out_queue, &o_msg, sizeof(out_msg) - sizeof(long), 0) == -1) {
    perror("msgsnd");
    exit(1);
  }

  destroy(l);
  exit(1);
}
void out_child(int queue) {
  out_msg msg;
  unsigned record_in1 = 0, record_in2 = 0;
  int val1 = 0, val2 = 0;

  while (1) {
    if (msgrcv(queue, &msg, sizeof(out_msg) - sizeof(long), 0, 0) == -1) {
      perror("msgrcv");
      exit(1);
    }

    if (msg.done)
      break;

    if (msg.id == IN1) {
      record_in1++;
      val1 += msg.e.value;
    } else if (msg.id == IN2) {
      record_in2++;
      val2 += msg.e.value;
    }
  }

  printf("OUT: ricevuti n.%d valori validi di IN1 con totale %d\n", record_in1,
         val1);
  printf("OUT: ricevuti n.%d valori validi di IN1 con totale %d\n", record_in2,
         val2);

  exit(0);
}

int main(int argc, char **argv) {
  if (argc < 4) {
    fprintf(stderr, "Usage: %s <db-file> <queries-file-1> <queries-file-2>\n",
            argv[0]);
    exit(1);
  }

  int queue1, queue2;

  if ((queue1 = msgget(IPC_PRIVATE, IPC_CREAT | 0600)) == -1) {
    perror("msgget");
    exit(1);
  }

  if ((queue2 = msgget(IPC_PRIVATE, IPC_CREAT | 0600)) == -1) {
    perror("msgget");
    exit(1);
  }

  // IN1
  if (fork() == 0)
    in_child(IN1, queue1, argv[2]);

  // IN2
  if (fork() == 0)
    in_child(IN2, queue1, argv[3]);

  // DB
  if (fork() == 0)
    db_child(queue1, queue2, argv[1]);

  // OUT
  if (fork() == 0)
    out_child(queue2);

  for (int i = 0; i < 4; i++)
    wait(NULL);

  msgctl(queue1, IPC_RMID, NULL);
  msgctl(queue2, IPC_RMID, NULL);
}