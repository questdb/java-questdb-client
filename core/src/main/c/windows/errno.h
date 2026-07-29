#ifndef ZLIB_ERRNO_H
#define ZLIB_ERRNO_H


static DWORD dwTlsIndexLastError = 0;

void SaveLastError();

/* Returns the per-thread error code most recently stored by SaveLastError().
 * Both functions are defined in os.c, the only translation unit whose static
 * dwTlsIndexLastError copy is initialised by TlsAlloc() in DllMain; every
 * other includer's copy of the variable is an unused zero. */
DWORD GetSavedLastError();

#endif //ZLIB_ERRNO_H
