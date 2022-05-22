/*
    Homework n.3

    Usando la possibilita' di mappare file in memoria, creare un programma che
    possa manipolare un file arbitrariamente grande costituito da una sequenza
    di record lunghi N byte.
    La manipolazione consiste nel riordinare, tramite un algoritmo di ordinamento
    a scelta, i record considerando il contenuto dello stesso come chiave:
    ovvero, supponendo N=5, il record [4a a4 91 f0 01] precede [4a ff 10 01 a3].
    La sintassi da supportare e' la seguente:
     $ homework-3 <N> <pathname del file>

    E' possibile testare il programma sul file 'esempio.txt' prodotto dal seguente
    comando, utilizzando il parametro N=33:
     $ ( for I in `seq 1000`; do echo $I | md5sum | cut -d' ' -f1 ; done ) > esempio.txt

    Su tale file, l'output atteso e' il seguente:
     $ homework-3 33 esempio.txt
     $ head -n5 esempio.txt
        000b64c5d808b7ae98718d6a191325b7
        0116a06b764c420b8464f2068f2441c8
        015b269d0f41db606bd2c724fb66545a
        01b2f7c1a89cfe5fe8c89fa0771f0fde
        01cdb6561bfb2fa34e4f870c90589125
     $ tail -n5 esempio.txt
        ff7345a22bc3605271ba122677d31cae
        ff7f2c85af133d62c53b36a83edf0fd5
        ffbee273c7bb76bb2d279aa9f36a43c5
        ffbfc1313c9c855a32f98d7c4374aabd
        ffd7e3b3836978b43da5378055843c67
*/

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>

char *swap_buffer;  // riferimento ad buffer di scambio globale

/* trova il pivot in map[i]...map[j] restituendone una copia (allocando la memoria);
   restituisce NULL se gli elementi sono tutti uguali                               */
char *search_pivot(char *map, int i, int j, int size) {
    int k;

    for (k=i+1; k<=j; k++) {
        if (memcmp(map+size*k, map+size*i, size) > 0)   // map[k] > map[i]
            return(memcpy(malloc(size), map+size*k, size));      // pivot = map[k]
        else if (memcmp(map+size*k, map+size*i, size) < 0) // map[k] < map[i]
            return(memcpy(malloc(size), map+size*i, size));    // pivot = map[i]
    }
    return(NULL);
}

// partiziona map[p]...map[r] usando il pivot
int partition(char *map, int p, int r, char *pivot, int size) {
    int i, j;

    i = p;
    j = r;
    do {
        while (memcmp(map+size*j, pivot, size) >= 0)   // map[j] >= pivot
            j--;
        while (memcmp(map+size*i, pivot, size) < 0)    // map[i] < pivot
            i++;
        if (i<j) {  // map[i] <-> map[j]
            memcpy(swap_buffer, map+size*i, size);
            memcpy(map+size*i, map+size*j, size);
            memcpy(map+size*j, swap_buffer, size);
        }
    } while (i<j);
    return(j);
}

// quicksort in versione ricorsiva
void quicksort(char *map, int p, int r, int size) {
    int q;
    char *pivot;

    pivot = search_pivot(map, p, r, size);
    if ( (p < r) && (pivot != NULL) ) {
        q = partition(map, p, r, pivot, size);
        quicksort(map, p, q, size);
        quicksort(map, q+1, r, size);
    }
    if (pivot) free(pivot);
}

int main(int argc, char *argv[]) {
    struct stat sb;
    int size, i, num_records;
    char *map;
    long fd;

    if (argc < 3) {
        fprintf(stderr, "uso: %s <dimensione record> <file>\n", argv[0]);
        exit(1);
    }

    if ((fd = open(argv[2], O_RDWR)) == -1) {
        perror(argv[2]);
        exit(1);
    }

    if (fstat(fd, &sb) == -1) {
        perror("fstat");
        exit(1);
    }
    if (!S_ISREG(sb.st_mode)) {
        fprintf(stderr, "%s non è un file\n", argv[2]);
        exit(1);
    }

    size = atoi(argv[1]);
    if ( (size <= 0) || ((sb.st_size % size) != 0) ) {
        fprintf(stderr, "dimensione del record %d non valida o dimensione del file non congruente!\n", size);
        exit(1);
    }

    if ((map = mmap(NULL, sb.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED) {
        perror("mmap");
        exit(1);
    }
    if (close(fd) == -1) {
        perror ("close");
        exit(1);
    }

    // alloca il buffer di scambio globale (al posto di uno locale alla procedura 'partition'
    swap_buffer = malloc(size);

    // ordina il contenuto del file
    num_records = sb.st_size / size;
    quicksort(map, 0, num_records-1, size);

    // libera la memoria del buffer una volta finito
    free(swap_buffer);

    printf("Record del file '%s' riordinati!\n", argv[2]);

    if (munmap(map, sb.st_size) == -1) {
        perror ("munmap");
        exit(1);
    }
}


