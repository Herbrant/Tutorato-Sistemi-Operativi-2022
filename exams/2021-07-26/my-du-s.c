#include <dirent.h>
#include <linux/limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#define SHM_SIZE sizeof(shm_data)
#define MSG_SIZE sizeof(msg) - sizeof(long)

enum { S_SCANNER, S_STATER };

typedef struct {
    unsigned id;
    char path[PATH_MAX];
    char done;
} shm_data;

typedef struct {
    long type;
    unsigned id;
    unsigned long value;
    char done;
} msg;

int WAIT(int sem_id, int sem_num) {
    struct sembuf ops[1] = {{sem_num, -1, 0}};
    return semop(sem_id, ops, 1);
}
int SIGNAL(int sem_id, int sem_num) {
    struct sembuf ops[1] = {{sem_num, +1, 0}};
    return semop(sem_id, ops, 1);
}

int init_shm() {
    int shm_des;

    if ((shm_des = shmget(IPC_PRIVATE, SHM_SIZE, IPC_CREAT | 0600)) == -1) {
        perror("shmget");
        exit(1);
    }

    return shm_des;
}

int init_sem() {
    int sem_des;

    if ((sem_des = semget(IPC_PRIVATE, 2, IPC_CREAT | 0600)) == -1) {
        perror("semget");
        exit(1);
    }

    if (semctl(sem_des, S_SCANNER, SETVAL, 1) == -1) {
        perror("semctl SETVAL S_SCANNER");
        exit(1);
    }

    if (semctl(sem_des, S_STATER, SETVAL, 0) == -1) {
        perror("semctl SETVAL S_STATER");
        exit(1);
    }

    return sem_des;
}

int init_queue() {
    int queue;

    if ((queue = msgget(IPC_PRIVATE, IPC_CREAT | 0600)) == -1) {
        perror("msgget");
        exit(1);
    }

    return queue;
}

void scanner(char id, int shm_des, int sem_des, char *path, char base) {
    DIR *d;
    struct dirent *dirent;
    shm_data *data;

    if ((data = (shm_data *)shmat(shm_des, NULL, 0)) == (shm_data *)-1) {
        perror("shmat");
        exit(1);
    }

    if ((d = opendir(path)) == NULL) {
        perror("opendir");
        exit(1);
    }

    while ((dirent = readdir(d))) {
        if (!strcmp(dirent->d_name, ".") || !strcmp(dirent->d_name, ".."))
            continue;
        else if (dirent->d_type == DT_REG) {
            WAIT(sem_des, S_SCANNER);
            sprintf(data->path, "%s/%s", path, dirent->d_name);
            data->done = 0;
            data->id = id;
            SIGNAL(sem_des, S_STATER);
        } else if (dirent->d_type == DT_DIR) {
            char tmp[PATH_MAX];
            sprintf(tmp, "%s/%s", path, dirent->d_name);
            scanner(id, shm_des, sem_des, tmp, 0);
        }
    }

    closedir(d);

    if (base) {
        WAIT(sem_des, S_SCANNER);
        data->done = 1;
        SIGNAL(sem_des, S_STATER);
        exit(0);
    }
}

void stater(int shm_des, int sem_des, int queue, unsigned n) {
    struct stat statbuf;
    shm_data *data;
    msg m;
    m.type = 1;
    m.done = 0;
    unsigned done_counter = 0;

    if ((data = shmat(shm_des, NULL, 0)) == (shm_data *)-1) {
        perror("shmat");
        exit(1);
    }

    while (1) {
        WAIT(sem_des, S_STATER);

        if (data->done) {
            done_counter++;

            if (done_counter == n)
                break;
            else {
                SIGNAL(sem_des, S_SCANNER);
                continue;
            }
        }

        if (stat(data->path, &statbuf) == -1) {
            perror("stat");
            exit(1);
        }

        m.id = data->id;
        m.value = statbuf.st_blocks;
        SIGNAL(sem_des, S_SCANNER);

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

    exit(0);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s [path-1] [path-2] [...]\n", argv[0]);
        exit(1);
    }

    int shm_des = init_shm();
    int sem_des = init_sem();
    int queue = init_queue();
    msg m;
    unsigned long blocks[argc - 1];

    // Stater
    if (!fork())
        stater(shm_des, sem_des, queue, argc - 1);

    // Scanners
    for (int i = 1; i < argc; i++)
        if (!fork())
            scanner(i - 1, shm_des, sem_des, argv[i], 1);

    for (int i = 0; i < argc - 1; i++)
        blocks[i] = 0;

    while (1) {
        if (msgrcv(queue, &m, MSG_SIZE, 0, 0) == -1) {
            perror("msgrcv");
            exit(1);
        }

        if (m.done)
            break;

        blocks[m.id] += m.value;
    }

    for (int i = 0; i < argc - 1; i++)
        printf("%ld %s\n", blocks[i], argv[i + 1]);

    shmctl(shm_des, IPC_RMID, 0);
    semctl(sem_des, 0, IPC_RMID, 0);
    msgctl(queue, IPC_RMID, 0);
}