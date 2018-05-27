#ifndef NODE_H
#define NODE_H

#include <mpi.h>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include "block.h"


#define TAG_NEW_BLOCK 10
#define TAG_CHAIN_HASH 21
#define TAG_CHAIN_RESPONSE 22
#define MAX_BLOCKS 200

extern MPI_Datatype* MPI_BLOCK;

void broadcast_block(const Block *block);
void* proof_of_work(void *ptr);
int node();
bool validate_block_for_chain(const Block *rBlock, const MPI_Status *status);
bool verificar_y_migrar_cadena(const Block *rBlock, const MPI_Status *status);

/* Semáforos para evitar que broadcast_block y 
    node se ejecuten concurrentemente */
//mutex broadcast_mtx;
//condition_variable brodcast_cond;
//atomic<bool> thread_recibeMensajes = false;

mutex recibeMensajes_mtx;
condition_variable recibeMensajes_cond;
atomic<bool> thread_broadcast = false;

#endif  // NODE_H
