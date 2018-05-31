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
#include <random>

int total_nodes, mpi_rank;
Block *last_block_in_chain;
map<string,Block> node_blocks;

//Semaforos y mutex
mutex lastBlockInChain_change_mtx;
mutex recibeMensajes_mtx;
condition_variable recibeMensajes_cond;
atomic<bool> thread_broadcast;


//Cuando me llega una cadena adelantada, y tengo que pedir los nodos que me faltan
//Si nos separan más de VALIDATION_BLOCKS bloques de distancia entre las cadenas, se descarta por seguridad
bool verificar_y_migrar_cadena(const Block *rBlock, const MPI_Status *status)
{
	// Envío mensaje TAG_CHAIN_HASH
	Block *blockchain = new Block[VALIDATION_BLOCKS];
	MPI_Send((void *) rBlock, 1, *MPI_BLOCK, rBlock->node_owner_number, TAG_CHAIN_HASH, MPI_COMM_WORLD);

	// Recibo mensaje TAG_CHAIN_RESPONSE
	MPI_Status status_recv;
	MPI_Recv(blockchain, VALIDATION_BLOCKS, *MPI_BLOCK, rBlock->node_owner_number, TAG_CHAIN_RESPONSE, MPI_COMM_WORLD, &status_recv);

	//Cantidad: status_recv.count
	printf("[%d] Recibo cadena de %d \n", mpi_rank, rBlock->node_owner_number);

	/* Verifico que los bloques recibidos
	   sean válidos y se puedan acoplar a la cadena */
	bool mismo_hash_rBlock_primero = (strcmp(blockchain[0].block_hash, rBlock->block_hash) == 0);
	bool mismo_indice_rBlock_primero = (blockchain[0].index == rBlock->index);
	string hash_hex_str;
	block_to_hash(&blockchain[0], hash_hex_str);
	bool hash_valido_primero = (hash_hex_str.compare(blockchain[0].block_hash) == 0);
	bool bloques_en_orden = true;

	for (int i = 0; i < VALIDATION_BLOCKS-1 && bloques_en_orden; ++i)
	{
		/* OJO!! Porque no siempre son la cantidad de VALIDATION_BLOCKs dado que 
				 podrian venir menos si tu index es 3 por ejemplo.. (vienen 2)
				 Ver en status_recv.count */
		if (strcmp(blockchain[i].previous_block_hash, blockchain[i+1].block_hash) == 1 ||
			blockchain[i].index != blockchain[i+1].index + 1)
		{
			bloques_en_orden = false;
		}
	}

	bool pude_migrar = false;

	if (mismo_hash_rBlock_primero && mismo_indice_rBlock_primero && 
		hash_valido_primero && bloques_en_orden)
	{
		// Veo si puedo reconstruir la cadena
		for (int i = 0; i < VALIDATION_BLOCKS && !pude_migrar; ++i)
		{
			/* Para cada elemento de la cadena que me mandaron, me 
			   fijo si ya lo tengo en node_blocks. Si ecuentro uno,
			   puedo reconstruir la cadena a partir de ese. */
			map<string,Block>::iterator actual_it = node_blocks.find(blockchain[i].block_hash);
			bool encontre = (actual_it != node_blocks.end());
			if (encontre)
			{
				for (int j = 0; j < i; ++j)
				{
					/* Agrego el bloque al diccionario */
					node_blocks[string(blockchain[j].block_hash)] = blockchain[j];
				}

				/* Actualizo mi último bloque */
				*last_block_in_chain = *rBlock;
				pude_migrar = true;
			}
		}
	}

	if(pude_migrar)
	{
		printf("[%d] Pude migrar la cadena de %d \n", mpi_rank, rBlock->node_owner_number);
	}
	else
	{
		printf("[%d] No pude migrar la cadena de %d:\n",mpi_rank, rBlock->node_owner_number);
	}
		

	delete []blockchain;
	return pude_migrar;
}


// Verifica que el bloque tenga que ser incluido en la cadena, y lo agrega si corresponde
bool validate_block_for_chain(const Block *rBlock, const MPI_Status *status)
{

	if(valid_new_block(rBlock))
	{
        // Mutex con proof_of_work para que no cambien el last_block_in_chain al mismo tiempo
	    lastBlockInChain_change_mtx.lock(); 

		/* Agrego el bloque al diccionario, aunque esto no
		   necesariamente lo agrega a la cadena */
		node_blocks[string(rBlock->block_hash)] = *rBlock;

		/* Si el índice del bloque recibido es 1 y mí último bloque actual 
		   tiene índice 0, entonces lo agrego como nuevo último. */
		if(rBlock->index == 1 && last_block_in_chain->index == 0)
		{
			*last_block_in_chain = *rBlock;
			printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
			lastBlockInChain_change_mtx.unlock(); //Fin de zona crítica

			return true;
		}

		// Si el índice del bloque recibido es el siguiente a mí último bloque actual
		bool siguienteAlActual = (rBlock->index == last_block_in_chain->index + 1);
		map<string,Block>::iterator anterior_rBLock = node_blocks.find(rBlock->previous_block_hash);
		bool encontreAnterior_rBLock = (anterior_rBLock != node_blocks.end());

		/* Si el índice del bloque recibido es el siguiente a mí último bloque 
		   actual, y el bloque anterior apuntado por el recibido es mí último actual, 
		   entonces lo agrego como nuevo último. */
		if(siguienteAlActual && encontreAnterior_rBLock && anterior_rBLock->second.index == last_block_in_chain->index)
		{
			*last_block_in_chain = *rBlock;
			printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
			lastBlockInChain_change_mtx.unlock(); //Fin de zona crítica

			return true;
		}

		/* Si el índice del bloque recibido es el siguiente a mí último bloque 
		   actual, pero el bloque anterior apuntado por el recibido no es mí último 
		   actual, entonces hay una blockchain más larga que la mía. */
		if(siguienteAlActual && !encontreAnterior_rBLock)
		{
			printf("[%d] Perdí la carrera por uno (%d) contra %d \n", mpi_rank, rBlock->index, status->MPI_SOURCE);
			bool res = verificar_y_migrar_cadena(rBlock,status);
			lastBlockInChain_change_mtx.unlock(); //Fin de zona crítica

			return res;
		}

		/* Si el índice del bloque recibido es igual al índice de mi último bloque 
		   actual, entonces hay dos posibles forks de la blockchain pero mantengo la mía */
		if(rBlock->index == last_block_in_chain->index)
		{
			printf("[%d] Conflicto suave: Conflicto de branch (%d) contra %d \n",mpi_rank,rBlock->index,status->MPI_SOURCE);
			lastBlockInChain_change_mtx.unlock(); //Fin de zona crítica

			return false;
		}

		/* Si el índice del bloque recibido es anterior al índice de mi último bloque 
		   actual, entonces lo descarto porque asumo que mi cadena es la que está quedando 
		   preservada. */
		if(rBlock->index < last_block_in_chain->index)
		{
			printf("[%d] Conflicto suave: Descarto el bloque (%d vs %d) contra %d \n",mpi_rank,rBlock->index,last_block_in_chain->index, status->MPI_SOURCE);
			lastBlockInChain_change_mtx.unlock(); //Fin de zona crítica

			return false;
		}

		/* Si el índice del bloque recibido está más de una posición adelantada a mi 
		   último bloque actual, entonces me conviene abandonar mi blockchain actual. */
		if(rBlock->index-1 > last_block_in_chain->index){
			printf("[%d] Perdí la carrera por varios contra %d \n", mpi_rank, status->MPI_SOURCE);
			bool res = verificar_y_migrar_cadena(rBlock,status);
			lastBlockInChain_change_mtx.unlock(); //Fin de zona crítica

			return res;
		}
	}

	lastBlockInChain_change_mtx.unlock(); //Fin de zona crítica
	printf("[%d] Error duro: Descarto el bloque recibido de %d porque no es válido \n",mpi_rank,status->MPI_SOURCE);

	return false;
}

int getRandom() {
	random_device rd; // obtain a random number from hardware
	mt19937 eng(rd()); // seed the generator
	uniform_int_distribution<> distr(0, total_nodes); // define the range
	return distr(eng);
}


// Envía el bloque minado a todos los nodos
void broadcast_block(const Block *block) {
	// No enviar a mí mismo
	MPI_Request requests[total_nodes-1];
	MPI_Status status[total_nodes-1];
	printf("[%d] broadcast_block Bloque: %d \n", mpi_rank, block->index);

	/* Envío a todos sin que me bloquee ya que después voy a esperar que 
	   todos lo hayan recibido bien antes de tocar el bloque */
	int orden = getRandom();
	for(int i=0; i<total_nodes; i++){
		int orden_normalizado =orden % total_nodes;
		if(orden_normalizado!=mpi_rank){
			int j = orden_normalizado;
			if(j>mpi_rank) //porque en request tengo que tener solo los total_nodes-1 requests
				j--;

			printf("[%d] broadcast_block a %d \n", mpi_rank, orden_normalizado);
			MPI_Isend((void *)block, 1, *MPI_BLOCK, orden_normalizado, TAG_NEW_BLOCK, MPI_COMM_WORLD, &requests[j]);
		}
		orden++;
	}
	
	MPI_Waitall(total_nodes-1, requests, status);

	//Listo, ahora puedo volver a guardar el bloque que acabo de minar y ponerme a minar de vuelta
}


//Proof of work
//TODO: Advertencia: puede tener condiciones de carrera
void* proof_of_work(void *ptr) {
	string hash_hex_str;
	Block block;
	unsigned int mined_blocks = 0;
	while(true){
		if(last_block_in_chain->index >= MAX_BLOCKS){
			printf("[%d] Maxima cantidad de bloques alcanzada, termino de minar \n",mpi_rank);
			break;
		}

		block = *last_block_in_chain;

		//Preparar nuevo bloque
		block.index += 1;
		block.node_owner_number = mpi_rank;
		block.difficulty = DEFAULT_DIFFICULTY;
		block.created_at = static_cast<unsigned long int> (time(NULL));
		memcpy(block.previous_block_hash,block.block_hash,HASH_SIZE);

		//Agregar un nonce al azar al bloque para intentar resolver el problema
		gen_random_nonce(block.nonce);

		//Hashear el contenido (con el nuevo nonce)
		block_to_hash(&block,hash_hex_str);

		//Contar la cantidad de ceros iniciales (con el nuevo nonce)
		if(solves_problem(hash_hex_str)){

			// Mutex con validate_block_for_chain para que no cambien el last_block_in_chain al mismo tiempo
			lastBlockInChain_change_mtx.lock();

			//Verifico que no haya cambiado mientras calculaba
			if(last_block_in_chain->index < block.index){
				mined_blocks += 1;
				*last_block_in_chain = block;
				strcpy(last_block_in_chain->block_hash, hash_hex_str.c_str());
				node_blocks[hash_hex_str] = *last_block_in_chain;
				printf("[%d] Agregué un producido con index %d \n",mpi_rank,last_block_in_chain->index);

				//CÁTEDRA: Mientras comunico, no responder mensajes de nuevos nodos

				thread_broadcast = true;
				
				broadcast_block(last_block_in_chain);

				// Despierto al thread que recibe mensajes
				thread_broadcast = false;
				recibeMensajes_cond.notify_one();
			}

			lastBlockInChain_change_mtx.unlock(); //Fin de zona crítica
		}
	}

	return NULL;
}

void envio_bloques(Block recvBlock, const MPI_Status *status){

	Block *blockchain = new Block[VALIDATION_BLOCKS];
	Block *lastBlock = &recvBlock;

	unsigned int i = 0;
	while(i<VALIDATION_BLOCKS){
		blockchain[i] = *lastBlock;
		
		if(lastBlock->index > 1){
			map<string,Block>::iterator anterior = node_blocks.find(lastBlock->previous_block_hash);
			lastBlock = &anterior->second;
		}else{
			break;
		}

		i++;
	}

	MPI_Send((void *) blockchain, i, *MPI_BLOCK, status->MPI_SOURCE, TAG_CHAIN_RESPONSE, MPI_COMM_WORLD);

	delete []blockchain;
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

	//CATEDRA: Crear thread para minar
	pthread_t thread_minador;
	pthread_attr_t thread_attr;

	// Initialize and set thread joinable
	pthread_attr_init(&thread_attr);
	pthread_attr_setdetachstate(&thread_attr, PTHREAD_CREATE_JOINABLE);

	if (pthread_create(&thread_minador, NULL, proof_of_work, NULL)) {
		printf("[%d] Error: unable to create thread \n", mpi_rank);
		return -1;
	}
	else {
		int recvFlag = -1;
		Block recvBlock;
		MPI_Request request;
		MPI_Status status;

		while (true) {

			if(last_block_in_chain->index >= MAX_BLOCKS){
				printf("[%d] Maxima cantidad de bloques alcanzada, no recibo mas mensajes \n",mpi_rank);
				break;
			}
			
			/* Me fijo si el semáforo me indica si se esta haciendo un broadcast de 
			   un nuevo nodo. Si es así espero a que se libere */
			unique_lock<mutex> lck(recibeMensajes_mtx);

			while (thread_broadcast){
				recibeMensajes_cond.wait(lck);
			}

			// Recibo mensajes de otros nodos
			if(recvFlag != 0){
				MPI_Irecv(&recvBlock, 1, *MPI_BLOCK, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
				recvFlag = 0;    
			}

			/* Si es un mensaje de nuevo bloque, llamar a la función validate_block_for_chain 
			   con el bloque recibido y el estado de MPI.
			   Si es un mensaje de pedido de cadena, respondo enviando los bloques correspondientes */
		
			MPI_Test(&request, &recvFlag, &status);

			//Si tengo un mensaje nuevo y no es mio (de mi thread)
			if (recvFlag != 0 && status.MPI_SOURCE != mpi_rank) {

				if(status.MPI_TAG == TAG_NEW_BLOCK){
					printf("[%d] Llegó un nuevo mesaje: Nuevo bloque minado!\n", mpi_rank);

					const Block toValidate = recvBlock;
					if (validate_block_for_chain(&toValidate, &status)){

					}
				}
				else if (status.MPI_TAG == TAG_CHAIN_HASH){
					printf("[%d] Llegó un nuevo mesaje: Pedido de cadena de %d\n", mpi_rank, status.MPI_SOURCE);
					//Envio los bloques correspondientes (Me piden la cadena)
					envio_bloques(recvBlock, &status);

				}//End if enviar cadena

				recvFlag = -1;
			}//End mensaje nuevo

		}//End While true

		// free attribute and wait for the other threads
		pthread_attr_destroy(&thread_attr);

		if (pthread_join(thread_minador, NULL)) {
			printf("[%d] Error: unable to join thread \n", mpi_rank);
			return -1;
		}
	}

	printf("[%d] Fin de ejecución \n",mpi_rank);

	delete last_block_in_chain;
	return 0;
}