#include "node.h"
#include "picosha2.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <cstdlib>
#include <queue>
#include <atomic>
#include <mpi.h>
#include <map>
#include <mutex>
#include <condition_variable>

int total_nodes, mpi_rank;
Block *last_block_in_chain;
map<string,Block> node_blocks;

//Cuando me llega una cadena adelantada, y tengo que pedir los nodos que me faltan
//Si nos separan más de VALIDATION_BLOCKS bloques de distancia entre las cadenas, se descarta por seguridad
bool verificar_y_migrar_cadena(const Block *rBlock, const MPI_Status *status){

  //TODO: Enviar mensaje TAG_CHAIN_HASH

  Block *blockchain = new Block[VALIDATION_BLOCKS];

  //TODO: Recibir mensaje TAG_CHAIN_RESPONSE

  //TODO: Verificar que los bloques recibidos
  //sean válidos y se puedan acoplar a la cadena
    //delete []blockchain;
    //return true;


  delete []blockchain;
  return false;
}

//Verifica que el bloque tenga que ser incluido en la cadena, y lo agrega si corresponde
bool validate_block_for_chain(const Block *rBlock, const MPI_Status *status){
    if(valid_new_block(rBlock)){

        //Agrego el bloque al diccionario, aunque no
        //necesariamente eso lo agrega a la cadena
        node_blocks[string(rBlock->block_hash)]=*rBlock;

        //TODO: Si el índice del bloque recibido es 1
        //y mí último bloque actual tiene índice 0,
        //entonces lo agrego como nuevo último.
        //printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
        //return true;

        //TODO: Si el índice del bloque recibido es
        //el siguiente a mí último bloque actual,
        //y el bloque anterior apuntado por el recibido es mí último actual,
        //entonces lo agrego como nuevo último.
        //printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
        //return true;

        //TODO: Si el índice del bloque recibido es
        //el siguiente a mí último bloque actual,
        //pero el bloque anterior apuntado por el recibido no es mí último actual,
        //entonces hay una blockchain más larga que la mía.
        //printf("[%d] Perdí la carrera por uno (%d) contra %d \n", mpi_rank, rBlock->index, status->MPI_SOURCE);
        //bool res = verificar_y_migrar_cadena(rBlock,status);
        //return res;


        //TODO: Si el índice del bloque recibido es igua al índice de mi último bloque actual,
        //entonces hay dos posibles forks de la blockchain pero mantengo la mía
        //printf("[%d] Conflicto suave: Conflicto de branch (%d) contra %d \n",mpi_rank,rBlock->index,status->MPI_SOURCE);
        //return false;

        //TODO: Si el índice del bloque recibido es anterior al índice de mi último bloque actual,
        //entonces lo descarto porque asumo que mi cadena es la que está quedando preservada.
        //printf("[%d] Conflicto suave: Descarto el bloque (%d vs %d) contra %d \n",mpi_rank,rBlock->index,last_block_in_chain->index, status->MPI_SOURCE);
        //return false;

        //TODO: Si el índice del bloque recibido está más de una posición adelantada a mi último bloque actual,
        //entonces me conviene abandonar mi blockchain actual
        //printf("[%d] Perdí la carrera por varios contra %d \n", mpi_rank, status->MPI_SOURCE);
        //bool res = verificar_y_migrar_cadena(rBlock,status);
        //return res;

    }

    printf("[%d] Error duro: Descarto el bloque recibido de %d porque no es válido \n",mpi_rank,status->MPI_SOURCE);
    return false;
}


//Envia el bloque minado a todos los nodos
void broadcast_block(const Block *block){
    //No enviar a mí mismo
    //TODO: Completar
    MPI_Request requests[total_nodes-1];
    MPI_Status status[total_nodes-1];
    printf("[%d] broadcast_block START\n", mpi_rank);
    //Envío a todos sin que me boloquee ya que después voy a esperar que todos lo hayan 
    //recibido bien antes de tocar el bloque
    for(int i=0; i<total_nodes; i++){
        if(i!=mpi_rank){
            int j=i;
            if(j>mpi_rank) //porque en request tengo que tener solo los total_nodes-1 requests
                j--;

            MPI_Isend((void *)block, 1, *MPI_BLOCK, i, TAG_NEW_BLOCK, MPI_COMM_WORLD, &requests[j]);
        }
    }
    
    MPI_Waitall(total_nodes-1, requests, status);
    //Listo, ahora puedo volver a guardar el bloque que acabé de minar y ponerme a minar de vuelta
    printf("[%d] broadcast_block END\n", mpi_rank);
}

//Proof of work
//TODO: Advertencia: puede tener condiciones de carrera
void* proof_of_work(void *ptr){
    //Vector de 4 punteros: total_nodes, mpi_rank, last_block_in_chain, node_blocks;
    // &total_nodes = (int *)ptr[0];
    // &mpi_rank = (int *) ptr[1];
    // last_block_in_chain = (Block *) ptr[2];
    // &node_blocks = *ptr[3];

    string hash_hex_str;
    Block block;
    unsigned int mined_blocks = 0;
    while(true){

        block = *last_block_in_chain;

        //Preparar nuevo bloque
        block.index += 1;
        block.node_owner_number = mpi_rank;
        block.difficulty = DEFAULT_DIFFICULTY;
        memcpy(block.previous_block_hash,block.block_hash,HASH_SIZE);

        //Agregar un nonce al azar al bloque para intentar resolver el problema
        gen_random_nonce(block.nonce);

        //Hashear el contenido (con el nuevo nonce)
        block_to_hash(&block,hash_hex_str);

        //Contar la cantidad de ceros iniciales (con el nuevo nonce)
        if(solves_problem(hash_hex_str)){

            //Verifico que no haya cambiado mientras calculaba
            if(last_block_in_chain->index < block.index){
                mined_blocks += 1;
                *last_block_in_chain = block;
                strcpy(last_block_in_chain->block_hash, hash_hex_str.c_str());
                last_block_in_chain->created_at = static_cast<unsigned long int> (time(NULL));
                node_blocks[hash_hex_str] = *last_block_in_chain;
                printf("[%d] Agregué un producido con index %d \n",mpi_rank,last_block_in_chain->index);

                //TODO: Mientras comunico, no responder mensajes de nuevos nodos

                /* Me fijo si el semáforo me indica si node esta recibiendo
                    un mensaje. Si es así, espero a que se libere */
                //unique_lock<mutex> lck(broadcast_mtx);
                //while (thread_recibeMensajes){
                //    broadcast_cond.wait(lck);
                //}

                thread_broadcast = true;
                
                broadcast_block(last_block_in_chain);

                // Despierto al thread que recibe mensajes
                thread_broadcast = false;
                recibeMensajes_cond.notify_one();                
            }
        }

    }

    return NULL;
}


int node(){

    //Tomar valor de mpi_rank y de nodos totales
    MPI_Comm_size(MPI_COMM_WORLD, &total_nodes);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    //La semilla de las funciones aleatorias depende del mpi_ranking
    srand(time(NULL) + mpi_rank);
    printf("[MPI] Lanzando proceso %u\n", mpi_rank);

    last_block_in_chain = new Block;

    //Inicializo el primer bloque
    last_block_in_chain->index = 0;
    last_block_in_chain->node_owner_number = mpi_rank;
    last_block_in_chain->difficulty = DEFAULT_DIFFICULTY;
    last_block_in_chain->created_at = static_cast<unsigned long int> (time(NULL));
    memset(last_block_in_chain->previous_block_hash,0,HASH_SIZE);

    //TODO: Crear thread para minar
    pthread_t thread_minador;
    pthread_attr_t thread_attr;
    int thread_response;
    void *thread_data[4];   //Vector de 4 punteros: total_nodes, mpi_rank, last_block_in_chain, node_blocks;
    thread_data[0] = &total_nodes;
    thread_data[1] = &mpi_rank;
    thread_data[2] = last_block_in_chain;
    thread_data[3] = &node_blocks;

    // Initialize and set thread joinable
    pthread_attr_init(&thread_attr);
    pthread_attr_setdetachstate(&thread_attr, PTHREAD_CREATE_JOINABLE);

    thread_response = pthread_create(&thread_minador, NULL, proof_of_work, &thread_data);
    if (thread_response) {
        printf("Error: unable to create thread \n");
        return -1;

    }else{
        int recvFlag = -1;
        Block newBlock;
        MPI_Request request;
        MPI_Status status;

        while(true){
            
            /* Me fijo si el semáforo me indica si se esta haciendo
                un broadcast de un nuevo nodo. Si es así espero
                a que se libere */
            unique_lock<mutex> lck(recibeMensajes_mtx);

            while (thread_broadcast){
                recibeMensajes_cond.wait(lck);
            }

            //TODO: Recibir mensajes de otros nodos
            if(recvFlag != 0){
                MPI_Irecv(&newBlock, 1, *MPI_BLOCK, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
                recvFlag = 0;    
            }

            //TODO: Si es un mensaje de nuevo bloque, llamar a la función
            // validate_block_for_chain con el bloque recibido y el estado de MPI

            //TODO: Si es un mensaje de pedido de cadena,
            //responderlo enviando los bloques correspondientes
            
            MPI_Test(&request, &recvFlag, &status);

            //Si tengo un mensaje nuevo y no es mio (de mi thread)
            if (recvFlag != 0 && status.MPI_SOURCE != mpi_rank) {
                
                if(status.MPI_TAG == TAG_NEW_BLOCK){
                    printf("[%d] Legó un nuevo mesaje: Nuevo bloque minado!\n", mpi_rank);

                    const Block toValidate = newBlock;
                    if( validate_block_for_chain(&toValidate, &status)){

                        if(node_blocks.size() == MAX_BLOCKS){
                            //Se llenó la cadena
                            break;
                        }else{
                            
                        }
                        
                    }else{
                        //Bloque no válido
                    }

                }else if (status.MPI_TAG == TAG_CHAIN_HASH){
                    printf("[%d] Legó un nuevo mesaje: Pedido de cadena\n", mpi_rank);
                    //Acá tengo que enviar los bloques correspondientes
                }

                recvFlag = -1;
            }           


        }

        // free attribute and wait for the other threads
        pthread_attr_destroy(&thread_attr);
        thread_response = pthread_join(thread_minador, NULL);
        if (thread_response) {
            printf("Error: unable to join thread \n");
            return -1;
        }

    }

    delete last_block_in_chain;
    return 0;
}