/*
    Homework n.4

    Estendere l'esercizio 'homework n.1' affinche' operi correttamente
    anche nel caso in cui tra le sorgenti e' indicata una directory, copiandone
    il contenuto ricorsivamente. Eventuali link simbolici incontrati dovranno
    essere replicati come tali (dovrà essere creato un link e si dovranno
    preservare tutti permessi di accesso originali dei file e directory).

    Una ipotetica invocazione potrebbe essere la seguente:
     $ homework-4 directory-di-esempio file-semplice.txt path/altra-dir/ "nome con spazi.pdf" directory-destinazione
*/

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <libgen.h>
#include <dirent.h>
#include <errno.h>

#define BUFSIZE 2048

void copy_element(const char *pathname, const char *destination, int depth) {
    int sd, dd, size, i;
    char buffer[BUFSIZE], buffer2[BUFSIZE];
    struct stat statbuf;
    DIR *dp;
    struct dirent *entry;

    /* prepara in 'buffer' il nome del file di destinazione:
       <directory destinazione>/<sorgente>                    */
    strncpy(buffer, destination, BUFSIZE);
    strncat(buffer, "/", BUFSIZE-strlen(buffer));
    strncpy(buffer2,  , BUFSIZE);    // ne faccio una copia, basename puo' alterarla
    strncat(buffer, basename(buffer2), BUFSIZE-strlen(buffer));

    if (lstat(pathname, &statbuf) == -1) {
        perror(pathname);
        exit(1);
    }

    // determina il tipo di oggetto da copiare
    switch (statbuf.st_mode & S_IFMT) {
    case S_IFLNK:   // e' un link simbolico
        // legge il pathname del link simbolico
        if ((size = readlink(pathname, buffer2, BUFSIZE)) == -1) {
            perror(pathname);
            exit(1);
        }
        buffer2[size] = '\0';

        printf("%*s%s (l)--> %s [=> %s]\n", depth, "  ", pathname, buffer, buffer2);

        // crea un nuovo link simbolico con lo stesso pathname interno
        if (symlink(buffer2, buffer) == -1) {
            perror(buffer);
            exit(1);
        }
        break;
    case S_IFREG:   // e' un file regolare
        printf("%*s%s (f)--> %s\n", depth, "  ", pathname, buffer);

        // apre il file sorgente di turno in sola lettura
        if ((sd = open(pathname, O_RDONLY)) == -1) {
            perror(pathname);
            exit(1);
        }

        // apre il file destinazione in sola scrittura, con troncamento e creazione
        if ((dd = open(buffer, O_WRONLY|O_CREAT|O_TRUNC, statbuf.st_mode & 0777)) == -1) {
            perror(buffer);
            exit(1);
        }

        // copia i dati dalla sorgente alla destinazione
        do {
            // legge fino ad un massimo di BUFSIZE byte dalla sorgente
            if ((size = read(sd, buffer, BUFSIZE)) == -1) {
                perror(pathname);
                exit(1);
            }
            // scrive i byte letti
            if (write(dd, buffer, size) == -1) {
                perror(destination);
                exit(1);
            }
        } while (size == BUFSIZE);

        // chiude i file attualmente aperti
        close(sd);
        close(dd);
        break;
    case S_IFDIR:   // e' una directory (da copiare ricorsivamente)
        printf("%*s%s/ (d)--> %s/\n", depth, "  ", pathname, buffer);

        if (mkdir(buffer, statbuf.st_mode & 0777) == -1) {
            perror(buffer);
            if (errno != EEXIST) exit(1);
        }
        // apre	la directory
        if ((dp = opendir(pathname)) == NULL) {
            perror(pathname);
            return;
        }
        // legge le varie voci
        while ((entry=readdir(dp)) != NULL) {
            if (strcmp(entry->d_name,".") == 0  || strcmp(entry->d_name,"..") == 0 )
                continue;
            /* ricostruisce in buffer2 il pathname dell'elemento
               interno alla directory da copiare ricorsivamente  */
            strncpy(buffer2, pathname, BUFSIZE);
            strncat(buffer2, "/", BUFSIZE-strlen(buffer2));
            strncat(buffer2, entry->d_name, BUFSIZE-strlen(buffer2));

            // copia ricorsivamente l'elemento della directory
            copy_element(buffer2, buffer, depth+1);
        }
        closedir(dp);
        break;
    default:
        fprintf(stderr, "tipo di oggetto non supportato!\n");
        exit(1);
    }
}

int main(int argc, char *argv[]) {
    int i;

    if (argc < 3) {
        printf("utilizzo: %s [<sorgente>...] <directory destinazione>\n", argv[0]);
        exit(1);
    }

    for (i = 1; i < argc-1; i++)
        copy_element(argv[i], argv[argc-1], 0);
}


