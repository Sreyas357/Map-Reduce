#include"map_reduce.h"
#include<pthread.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>


#define MAX_WORD_LENGTH 20
#define MAX_WORKERS 100
#define MAX_SIZE 10000
#define MAX_GROUPS 100
#define MAX_KEYS 1000
#define TABLE_SIZE 1000
#define MAX_COLLISION 1000

Mapper map_f;
Reducer reduce_f;

typedef struct {
    char key[MAX_WORD_LENGTH];
    char value[MAX_WORD_LENGTH];
}pm; // structure defined for storing (key,value) pairs emitted by MR_emit

typedef struct{
    int num;
    char string[MAX_WORD_LENGTH];
}entry;

typedef struct{
    int p;
    entry*list;
}ht;       // structure for storing hash table


pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;   //lock for workers doing map and reduce tasks
pthread_mutex_t group_lock[MAX_GROUPS] = {PTHREAD_MUTEX_INITIALIZER}; //lock for emiting during MR_emit

pthread_cond_t master = PTHREAD_COND_INITIALIZER;
pthread_cond_t worker = PTHREAD_COND_INITIALIZER;

int total_mappers, number_of_mappers_done,number_of_reducers_done,total_partitions,total_keys;
Partitioner partition_function;


ht *hash_table; 

pm**memory; //memory for storing (key,value) pairs emitted by MR_emit
int*present_index; //array which stores counter for partitions so to define memory where (key,value) pair to be stored

char**argv_global;  // global variable for storing argv pointer given in MR_run

int*counter;  // stores present counter used in get_next
entry*all_keys; //stores all keys ,along with partion number in structure


int compare(const void *a,const void*b){
    pm*x = (pm*)a;
    pm*y = (pm*)b;
    return strcmp(x->key,y->key);
}

int*partition_on_keys(pm*array,int size){
    int*keys_indices = (int*)malloc(sizeof(int)*(MAX_KEYS+1));

    int k=1;
    keys_indices[k++]= 0;

    for(int i=1 ;i<size;i++){
        if(strcmp(array[i].key,array[i-1].key) !=0){
            keys_indices[k++]= i;
        }
    }
    
    total_keys += k-1;

    keys_indices[0] = k-1 ; //key_indices[0] -> stores number of diff keys 

    return keys_indices;
}


void*sort_pm_array(void*x){
    int i = *((int*)x); 
    if(present_index[i] == 0){return NULL;} //present_index[i] -> size of partition i
    qsort(memory[i],present_index[i],sizeof(pm),compare); 
    
    int*indices = partition_on_keys(memory[i],present_index[i]);
    return (void*)indices;
}



unsigned long default_partition(char*key,int num_partitions){
    
    unsigned long hash = 5381;
    int c;
    while( (c = *(key++) ) != '\0' ){

        hash = hash*33 + c;
        hash = hash%num_partitions;

    }
    return hash;
}

unsigned int hash(const char *key) {
    unsigned long int value = 0;
    unsigned int i = 0;
    unsigned int key_len = strlen(key);

    for (; i < key_len; ++i) {
        value = value * 37 + key[i];
    }
    value = value % TABLE_SIZE;

    return value;
}

int str_to_int(char*key){
    int n = hash(key);
    entry*list = hash_table[n].list;

    for(int i = 0 ; i< MAX_COLLISION ;i++){
        if(strcmp(list[i].string,key )==0){
            return list[i].num;
        }
    }
    return -1;
}
 


void* map_worker(void*x){

    
    while(1){
        
        pthread_mutex_lock(&lock);

        if( number_of_mappers_done == total_mappers){
            pthread_mutex_unlock(&lock);
            return NULL ;
        }
        
        int present = number_of_mappers_done++ ;

        pthread_mutex_unlock(&lock);
        

        map_f(argv_global[present+1]);


    }
    return NULL;

}



void MR_emit(char*key,char*value){


    int group_no = partition_function(key,total_partitions);


    pm temp;
    memcpy(&temp.key,key,strlen(key)+1);
    memcpy(&temp.value,value,strlen(value)+1);

    pthread_mutex_lock(group_lock+group_no);

        memcpy(&memory[group_no][present_index[group_no]],&temp,sizeof(temp));
        present_index[group_no]++;

    pthread_mutex_unlock(group_lock+group_no);

}

char*get_next(char*key,int partition_number){

    int present_counter = counter[str_to_int(key)]++;

    if(present_counter== -1){
        return NULL;
    }

    if( present_counter == present_index[partition_number]-1){
        counter[str_to_int(key)] = -1;
    }
    
    else if( strcmp(memory[partition_number][present_counter].key ,memory[partition_number][present_counter+1].key) != 0){
        counter[str_to_int(key)] = -1;
    }

    return memory[partition_number][present_counter].value;
}

void* reduce_worker(void*x){

    pthread_mutex_lock(&lock);

    while(1){

        if( number_of_reducers_done == total_keys){
            pthread_mutex_unlock(&lock);
            return NULL ;
        }
        
        int present = number_of_reducers_done++;

        pthread_mutex_unlock(&lock);

        reduce_f(all_keys[present].string,get_next,all_keys[present].num);

    }
    return NULL;

}


void MR_run(int argc , char*argv[] ,Mapper map,int num_mappers , Reducer reduce , int num_reducers,Partitioner partition,int num_partitions){

    if( partition != NULL){
        partition_function = partition;
    }
    else{
        partition_function = default_partition;
    }
    total_mappers = argc;
    number_of_mappers_done = 0;
    argv_global = argv;
    total_keys = 0;

    map_f = map;
    reduce_f = reduce;

    total_partitions = num_partitions;

    memory = ( pm**)malloc( total_partitions*sizeof(pm*));

    for(int i =0 ; i< num_partitions ; i++){
        memory[i] = ( pm*)malloc(MAX_SIZE*sizeof(pm));
    }

    present_index = (int*)calloc(total_partitions,sizeof(int));

    pthread_t p[num_mappers];

    for(int i =0 ;i < num_mappers ;i++){
        pthread_create(&p[i],NULL,map_worker,NULL);
    }
    
    for(int i =0 ;i < num_mappers ;i++){
        pthread_join(p[i],NULL);
    }

    //printf("mapping part done \n");

    

    //mapping part done ,now sorting all partition regions parralelly

    pthread_t w[total_partitions];

    int x[total_partitions];

    for(int i=0;i<total_partitions;i++){x[i]=i;}

    for(int i=0;i<total_partitions;i++){
        pthread_create(&w[i],NULL,sort_pm_array,x+i);
    }

    int*partion_indices[num_partitions];

    for(int i=0;i<total_partitions;i++){
        pthread_join(w[i],(void*)(partion_indices+i));
    }


    //printf("sorting part done %d\n",total_keys);

    // for(int i= 0 ;i<num_partitions;i++){
    //     for(int j = 0 ; j < 20 ;j++){
    //         printf("%s %s \n",memory[i][j].key,memory[i][j].value);
    //     }
    // }


    //sorting part done
    

    counter = (int*)malloc(sizeof(int)*total_keys);

    hash_table = (ht*)malloc(sizeof(ht)*TABLE_SIZE);
    all_keys = (entry*)malloc(sizeof(entry)*total_keys);

    int current=0;

    for(int i = 0 ; i < num_partitions ; i++){

        if(present_index[i]==0 ){continue;}

        int n_k = partion_indices[i][0];


        for(int j = 1 ; j <= n_k ; j++){
    
            char*key = memory[i][partion_indices[i][j]].key;


            memcpy(all_keys[current].string,key,strlen(key)+1);
            all_keys[current].num = i;

            counter[current] = partion_indices[i][j];

            int n = hash(key);

            if(hash_table[n].list == NULL){
                hash_table[n].list = (entry*)malloc(sizeof(entry)*MAX_COLLISION);
            }
            memcpy(hash_table[n].list[hash_table[n].p].string,key,strlen(key)+1);
            hash_table[n].list[hash_table[n].p++].num = current++;

        }

    }

    // for(int i=0;i<total_keys;i++){
    //     printf("%s %d \n",all_keys[i].string,all_keys[i].num);
    // }

    //printf("reducing part started \n");

    number_of_reducers_done = 0;
    pthread_t r[num_reducers];

    for(int i =0 ;i < num_reducers ;i++){
        pthread_create(&r[i],NULL,reduce_worker,NULL);
    }

    for(int i =0 ;i < num_reducers ;i++){
        pthread_join(r[i],NULL);
    }


}