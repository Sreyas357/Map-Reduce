typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);


void MR_emit(char*key,char* value);

unsigned long default_partition(char*key,int num_partitions);

void MR_run(int argc , char*argv[] ,Mapper map,int num_mappers , Reducer reduce , int num_reducer,Partitioner partition,int num);