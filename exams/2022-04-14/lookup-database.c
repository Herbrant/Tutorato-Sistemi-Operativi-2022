#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#define BUFFER_SIZE 2048
#define IN1_ID 1
#define IN2_ID 2

typedef struct {
  long type;
  char key[BUFFER_SIZE];
  char done;
  char id;
} query_msg;

typedef struct {
  char key[BUFFER_SIZE];
  int value;
} entry;

typedef struct {
  long type;
  entry e;
  char process;
  char done;
} out_msg;

void in_child(char id, int queue, char *path) {
  FILE *f;
  query_msg msg;

  msg.type = 1;
  msg.done = 0;
  msg.id = id;

  if ((f = fopen(path, "r")) == NULL) {
    perror("fopen");
    exit(1);
  }

  unsigned query_number = 0;

  while (fgets(msg.key, BUFFER_SIZE, f)) {
    query_number++;

    if (msg.key[strlen(msg.key) - 1] == '\n')
      msg.key[strlen(msg.key) - 1] = '\0';

    printf("IN%d: inviata query n.%d '%s'\n", id, query_number, msg.key);
    if (msgsnd(queue, &msg, sizeof(query_msg) - sizeof(long), 0) == -1) {
      perror("msgsnd");
      exit(1);
    }
  }

  msg.done = 1;
  if (msgsnd(queue, &msg, sizeof(query_msg) - sizeof(long), 0) == -1) {
    perror("msgsnd");
    exit(1);
  }

  fclose(f);
  exit(0);
}

unsigned get_number_of_lines(char *path) {
  FILE *f;
  unsigned nlines = 0;
  char buffer[BUFFER_SIZE];

  if ((f = fopen(path, "r")) == NULL) {
    perror("fopen");
    exit(1);
  }

  while ((fgets(buffer, BUFFER_SIZE, f)))
    nlines++;

  fclose(f);
  return nlines;
}

entry create_entry(char *data) {
  char *key;
  char *value;
  entry e;

  if ((key = strtok(data, ":")) != NULL)
    if ((value = strtok(NULL, ":")) != NULL) {
      strncpy(e.key, key, BUFFER_SIZE);
      e.value = atoi(value);
    }

  return e;
}

entry *load_database(char *path, unsigned nlines) {
  FILE *f;
  if ((f = fopen(path, "r")) == NULL) {
    perror("fopen");
    exit(1);
  }

  entry *database = malloc(sizeof(entry) * nlines);
  char buffer[BUFFER_SIZE];

  for (int i = 0; i < nlines; i++) {
    fgets(buffer, BUFFER_SIZE, f);
    database[i] = create_entry(buffer);
  }

  fclose(f);
  return database;
}

int search_entry_index(entry *e, unsigned size, char *key) {
  for (int i = 0; i < size; i++)
    if (!strcmp(e[i].key, key))
      return i;

  return -1;
}

void db_child(int in_queue, int out_queue, char *path) {
  query_msg q_msg;
  out_msg o_msg;
  o_msg.type = 1;
  o_msg.done = 0;
  char done_counter = 0;
  int entry_index;
  unsigned nlines = get_number_of_lines(path);
  entry *database = load_database(path, nlines);
  printf("DB: letti n.%d record da file\n", nlines);

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

    entry_index = search_entry_index(database, nlines, q_msg.key);
    if (entry_index < 0)
      printf("DB: query '%s' da IN%d non trovata\n", q_msg.key, q_msg.id);
    else {
      printf("DB: query '%s' da IN%d trovata con valore %d\n", q_msg.key,
             q_msg.id, database[entry_index].value);

      o_msg.e = database[entry_index];
      o_msg.process = q_msg.id;
      if (msgsnd(out_queue, &o_msg, sizeof(out_msg) - sizeof(long), 0) == -1) {
        perror("msgsnd");
        exit(1);
      }
    }
  }

  o_msg.done = 1;
  if (msgsnd(out_queue, &o_msg, sizeof(out_msg) - sizeof(long), 0) == -1) {
    perror("msgsnd");
    exit(1);
  }

  free(database);
  exit(0);
}

void out_child(int queue) {
  unsigned record_in1 = 0, record_in2 = 0;
  int val1 = 0, val2 = 0;
  out_msg msg;

  while (1) {
    if (msgrcv(queue, &msg, sizeof(out_msg) - sizeof(long), 0, 0) == -1) {
      perror("msgrcv");
      exit(1);
    }

    if (msg.done)
      break;

    if (msg.process == IN1_ID) {
      record_in1++;
      val1 += msg.e.value;
    } else if (msg.process == IN2_ID) {
      record_in2++;
      val2 += msg.e.value;
    }
  }

  printf("OUT: ricevuti n.%d valori validi per IN1 con totale %d\n", record_in1,
         val1);
  printf("OUT: ricevuti n.%d valori validi per IN2 con totale %d\n", record_in2,
         val2);
  exit(0);
}

int main(int argc, char **argv) {
  if (argc < 4) {
    fprintf(stderr, "Usage: %s <db-file> <query-file-1> <query-file-2>\n",
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

  // DB
  if (fork() == 0)
    db_child(queue1, queue2, argv[1]);

  // IN1
  if (fork() == 0)
    in_child(IN1_ID, queue1, argv[2]);

  // IN2
  if (fork() == 0)
    in_child(IN2_ID, queue1, argv[3]);

  // OUT
  if (fork() == 0)
    out_child(queue2);

  for (int i = 0; i < 4; i++)
    wait(NULL);

  msgctl(queue1, IPC_RMID, NULL);
  msgctl(queue2, IPC_RMID, NULL);
}