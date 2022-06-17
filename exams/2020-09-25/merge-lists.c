#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/msg.h>
#include <sys/wait.h>
#include <unistd.h>
#define BUFFER_SIZE 32
#define MSG_SIZE sizeof(msg) - sizeof(long)

typedef struct {
    long type;
    char buffer[BUFFER_SIZE];
    char done;
} msg;

typedef struct {
    char word[BUFFER_SIZE];
    struct node *next;
} node;

typedef node *list;

list insert(list l, char *word) {
    node *n = malloc(sizeof(node));
    strncpy(n->word, word, BUFFER_SIZE);
    n->next = NULL;

    if (l == NULL)
        return n;

    node *ptr = l;

    while (ptr->next != NULL)
        ptr = (node *)ptr->next;

    ptr->next = (struct node *)n;

    return l;
}

char search(list l, char *word) {
    node *ptr = l;

    while (ptr != NULL) {
        if (!strcasecmp(ptr->word, word))
            return 1;

        ptr = (node *)ptr->next;
    }

    return 0;
}

void destroy(list l) {
    node *ptr = l;
    node *tmp;

    while (ptr != NULL) {
        tmp = (node *)ptr->next;
        free(ptr);
        ptr = tmp;
    }
}

void print(list l) {
    node *ptr = l;

    while (ptr != NULL) {
        printf("%s\n", ptr->word);
        ptr = (node *)ptr->next;
    }
}

void reader_child(int queue, char *path) {
    FILE *f;
    msg m;
    m.done = 0;
    m.type = 1;

    if ((f = fopen(path, "r")) == NULL) {
        perror("fopen");
        exit(1);
    }

    while (fgets(m.buffer, BUFFER_SIZE, f)) {
        if (isspace(m.buffer[0]))
            memmove(m.buffer, m.buffer + 1, strlen(m.buffer));

        if (isspace(m.buffer[strlen(m.buffer) - 1]))
            m.buffer[strlen(m.buffer) - 1] = '\0';

        if (msgsnd(queue, &m, MSG_SIZE, 0) == -1) {
            perror("msgsnd");
            exit(1);
        }
    }

    m.done = 1;
    if (msgsnd(queue, &m, MSG_SIZE, 0) == -1) {
        perror("msgsnd");
        exit(1);
    }

    fclose(f);
    exit(0);
}

void writer_child(int piped) {
    char buffer[BUFFER_SIZE];
    FILE *f;

    if ((f = fdopen(piped, "r")) == NULL) {
        perror("fdopen");
        exit(1);
    }

    while (fgets(buffer, BUFFER_SIZE, f))
        printf("%s", buffer);

    exit(0);
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <file-1> <file-2>\n", argv[0]);
        exit(1);
    }

    int queue;
    int pipe_fds[2];
    msg m;
    char done_counter = 0;
    list l;
    FILE *f;

    if ((queue = msgget(IPC_PRIVATE, IPC_CREAT | 0600)) == -1) {
        perror("msgget");
        exit(1);
    }

    // Reader 1
    if (!fork())
        reader_child(queue, argv[1]);

    // Reader 2
    if (!fork())
        reader_child(queue, argv[2]);

    if (pipe(pipe_fds) == -1) {
        perror("pipe");
        exit(1);
    }

    // Writer
    if (!fork()) {
        close(pipe_fds[1]);
        writer_child(pipe_fds[0]);
    }

    close(pipe_fds[0]);

    while (1) {
        if (msgrcv(queue, &m, MSG_SIZE, 0, 0) == -1) {
            perror("msgrcv");
            exit(1);
        }

        if (m.done) {
            done_counter++;

            if (done_counter > 1)
                break;
            else
                continue;
        }

        if (!search(l, m.buffer)) {
            l = insert(l, m.buffer);
            dprintf(pipe_fds[1], "%s\n", m.buffer);
        }
    }

    destroy(l);
    msgctl(queue, IPC_RMID, NULL);
    close(pipe_fds[1]);
}