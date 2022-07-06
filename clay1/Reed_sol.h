#ifndef REED_SOL_H_INCLUDED
#define REED_SOL_H_INCLUDED

extern int *reed_sol_vandermonde_coding_matrix(int k, int m, int w);
extern int *reed_sol_extended_vandermonde_matrix(int rows, int cols, int w);
extern int *reed_sol_big_vandermonde_distribution_matrix(int rows, int cols, int w);

extern int reed_sol_r6_encode(int k, int w, char **data_ptrs, char **coding_ptrs, int size);
extern int *reed_sol_r6_coding_matrix(int k, int w);

extern void reed_sol_galois_w08_region_multby_2(char *region, int nbytes);
extern void reed_sol_galois_w16_region_multby_2(char *region, int nbytes);
extern void reed_sol_galois_w32_region_multby_2(char *region, int nbytes);


#endif // REED_SOL_H_INCLUDED
