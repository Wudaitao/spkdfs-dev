#ifndef JERASURE_H_INCLUDED
#define JERASURE_H_INCLUDED
#include <iostream>
#include <map>
#include <set>
#include <list>
#include "Galois.h"
#include "common.h"
using namespace std;

typedef list<char *> bufferlist;////对buffelist定义，即为多个字符串的列表

int scalar_decode_chunks(int k, int m, int w, const set<int> &want_to_read, const map<int, bufferlist> &chunks, map<int, bufferlist> *decoded,int subsize);

int jerasure_decode(int k, int m, int w, int *erasures, char **data, char **coding, int blocksize);

int scalar_encode_chunks(int k, int m, int w, map<int, bufferlist> *decoded,int subsize);

int jerasure_encode(int k, int m, int w, char **data, char **coding, int blocksize);

int *liberation_coding_bitmatrix(int k, int w);

int *jerasure_matrix_to_bitmatrix(int k, int m, int w, int *matrix);
int **jerasure_dumb_bitmatrix_to_schedule(int k, int m, int w, int *bitmatrix);
int **jerasure_smart_bitmatrix_to_schedule(int k, int m, int w, int *bitmatrix);
int ***jerasure_generate_schedule_cache(int k, int m, int w, int *bitmatrix, int smart);

void jerasure_free_schedule(int **schedule);
void jerasure_free_schedule_cache(int k, int m, int ***cache);


/* ------------------------------------------------------------ */
/* Encoding - these are all straightforward.  jerasure_matrix_encode only
   works with w = 8|16|32.  */

void jerasure_do_parity(int k, char **data_ptrs, char *parity_ptr, int size);

void jerasure_matrix_encode(int k, int m, int w, int *matrix,
                            char **data_ptrs, char **coding_ptrs, int size);

void jerasure_bitmatrix_encode(int k, int m, int w, int *bitmatrix,
                               char **data_ptrs, char **coding_ptrs, int size, int packetsize);

void jerasure_schedule_encode(int k, int m, int w, int **schedule,
                              char **data_ptrs, char **coding_ptrs, int size, int packetsize);

/* ------------------------------------------------------------ */
/* Decoding. -------------------------------------------------- */

/* These return integers, because the matrix may not be invertible.

   The parameter row_k_ones should be set to 1 if row k of the matrix
   (or rows kw to (k+1)w+1) of th distribution matrix are all ones
   (or all identity matrices).  Then you can improve the performance
   of decoding when there is more than one failure, and the parity
   device didn't fail.  You do it by decoding all but one of the data
   devices, and then decoding the last data device from the data devices
   and the parity device.

   jerasure_schedule_decode_lazy generates the schedule on the fly.

   jerasure_matrix_decode only works when w = 8|16|32.

   jerasure_make_decoding_matrix/bitmatrix make the k*k decoding matrix
         (or wk*wk bitmatrix) by taking the rows corresponding to k
         non-erased devices of the distribution matrix, and then
         inverting that matrix.

         You should already have allocated the decoding matrix and
         dm_ids, which is a vector of k integers.  These will be
         filled in appropriately.  dm_ids[i] is the id of element
         i of the survivors vector.  I.e. row i of the decoding matrix
         times dm_ids equals data drive i.

         Both of these routines take "erased" instead of "erasures".
         Erased is a vector with k+m elements, which has 0 or 1 for
         each device's id, according to whether the device is erased.

   jerasure_erasures_to_erased allocates and returns erased from erasures.

 */

int jerasure_matrix_decode(int k, int m, int w,
                           int *matrix, int row_k_ones, int *erasures,
                           char **data_ptrs, char **coding_ptrs, int size);

int jerasure_bitmatrix_decode(int k, int m, int w,
                              int *bitmatrix, int row_k_ones, int *erasures,
                              char **data_ptrs, char **coding_ptrs, int size, int packetsize);

int jerasure_schedule_decode_lazy(int k, int m, int w, int *bitmatrix, int *erasures,
                                  char **data_ptrs, char **coding_ptrs, int size, int packetsize,
                                  int smart);

int jerasure_schedule_decode_cache(int k, int m, int w, int ***scache, int *erasures,
                                   char **data_ptrs, char **coding_ptrs, int size, int packetsize);

int jerasure_make_decoding_matrix(int k, int m, int w, int *matrix, int *erased,
                                  int *decoding_matrix, int *dm_ids);

int jerasure_make_decoding_bitmatrix(int k, int m, int w, int *matrix, int *erased,
                                     int *decoding_matrix, int *dm_ids);

int *jerasure_erasures_to_erased(int k, int m, int *erasures);

/* ------------------------------------------------------------ */
/* These perform dot products and schedules. -------------------*/
/*
   src_ids is a matrix of k id's (0 - k-1 for data devices, k - k+m-1
   for coding devices) that identify the source devices.  Dest_id is
   the id of the destination device.

   jerasure_matrix_dotprod only works when w = 8|16|32.

   jerasure_do_scheduled_operations executes the schedule on w*packetsize worth of
   bytes from each device.  ptrs is an array of pointers which should have as many
   elements as the highest referenced device in the schedule.

 */

void jerasure_matrix_dotprod(int k, int w, int *matrix_row,
                             int *src_ids, int dest_id,
                             char **data_ptrs, char **coding_ptrs, int size);

void jerasure_bitmatrix_dotprod(int k, int w, int *bitmatrix_row,
                                int *src_ids, int dest_id,
                                char **data_ptrs, char **coding_ptrs, int size, int packetsize);

void jerasure_do_scheduled_operations(char **ptrs, int **schedule, int packetsize);

/* ------------------------------------------------------------ */
/* Matrix Inversion ------------------------------------------- */
/*
   The two matrix inversion functions work on rows*rows matrices of
   ints.  If a bitmatrix, then each int will just be zero or one.
   Otherwise, they will be elements of gf(2^w).  Obviously, you can
   do bit matrices with crs_invert_matrix() and set w = 1, but
   crs_invert_bitmatrix will be more efficient.

   The two invertible functions return whether a matrix is invertible.
   They are more efficient than the inverstion functions.

   Mat will be destroyed when the matrix inversion or invertible
   testing is done.  Sorry.

   Inv must be allocated by the caller.

   The two invert_matrix functions return 0 on success, and -1 if the
   matrix is uninvertible.

   The two invertible function simply return whether the matrix is
   invertible.  (0 or 1). Mat will be destroyed.
 */

int jerasure_invert_matrix(int *mat, int *inv, int rows, int w);
int jerasure_invert_bitmatrix(int *mat, int *inv, int rows);
int jerasure_invertible_matrix(int *mat, int rows, int w);
int jerasure_invertible_bitmatrix(int *mat, int rows);

/* ------------------------------------------------------------ */
/* Basic matrix operations -------------------------------------*/
/*
   Each of the print_matrix routines require a w.  In jerasure_print_matrix,
   this is to calculate the field width.  In jerasure_print_bitmatrix, it is
   to put spaces between the bits.

   jerasure_matrix_multiply is a simple matrix multiplier in GF(2^w).  It returns a r1*c2
   matrix, which is the product of the two input matrices.  It allocates
   the product.  Obviously, c1 should equal r2.  However, this is not
   validated by the procedure.
*/

void jerasure_print_matrix(int *matrix, int rows, int cols, int w);
void jerasure_print_bitmatrix(int *matrix, int rows, int cols, int w);


int *jerasure_matrix_multiply(int *m1, int *m2, int r1, int c1, int r2, int c2, int w);

/* ------------------------------------------------------------ */
/* Stats ------------------------------------------------------ */
/*
  jerasure_get_stats fills in a vector of three doubles:

      fill_in[0] is the number of bytes that have been XOR'd
      fill_in[1] is the number of bytes that have been copied
      fill_in[2] is the number of bytes that have been multiplied
                 by a constant in GF(2^w)

  When jerasure_get_stats() is called, it resets its values.
 */

void jerasure_get_stats(double *fill_in);

#endif // JERASURE_H_INCLUDED
