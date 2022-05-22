/*
    Homework n.1

    Scrivere un programma in linguaggio C che permetta di copiare un numero
    arbitrario di file regolari su una directory di destinazione preesistente.

    Il programma dovra' accettare una sintassi del tipo:
     $ homework-1 file1.txt path/file2.txt "nome con spazi.pdf" directory-destinazione
*/

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#define BUFSIZE 4096
#define MODE 0660

char* create_dest_path(char* source, char* dest) {
    char* path = malloc(BUFSIZE);
    char *p1, *p2;

    strncpy(path, dest, BUFSIZE);
    int size = strlen(path);

    strncpy(path + size, "/", BUFSIZE - size);
    size++;

    p1 = p2 = source;
    while (*p2 != '\0') {
        if (*p2 == '/') p1 = p2+1;
        p2++;
    }

    strncpy(path + size, p1, BUFSIZE - size);

    return path;
}


void copy(char* source, char* dest) {
    int size, sd, dd;
    char buffer[BUFSIZ];

    printf("%s\t--> ", source);
    if ((sd = open(source, O_RDONLY)) == -1) {
        perror(source);
        exit(1);
    }

    char* path = create_dest_path(source, dest);

    printf("%s\n", path);
    if ((dd = open(path, O_WRONLY|O_CREAT|O_TRUNC, MODE)) == -1) {
        perror(path);
        free(path);
        exit(1);
    }

    do {
        if ((size = read(sd, buffer, BUFSIZE)) == -1) {
            perror(source);
            free(path);
            exit(1);
        }

        if (write(dd, buffer, size) == -1) {
            perror(path);
            free(path);
            exit(1);
        }
    } while (size == BUFSIZE);

    close(sd);
    close(dd);
    free(path);
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        printf("utilizzo: %s [<sorgente>...] <directory destinazione>\n", argv[0]);
        exit(1);
    }

    for (int i = 1; i < argc-1; i++)
        copy(argv[i], argv[argc - 1]);
}
