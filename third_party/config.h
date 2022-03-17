/* config.h.  Generated from config.h.in by configure.  */
/* config.h.in.  Generated from configure.ac by autoheader.  */

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the `memset' function. */
#define HAVE_MEMSET 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if the system has the type `_Bool'. */
/* #undef HAVE__BOOL */

/* Name of package */
#define PACKAGE "cedar"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "ynaga@tkl.iis.u-tokyo.ac.jp"

/* Define to the full name of this package. */
#define PACKAGE_NAME "cedar"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "cedar 2014-06-24"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "cedar"

/* Define to the home page for this package. */
#define PACKAGE_URL ""

/* Define to the version of this package. */
#define PACKAGE_VERSION "2014-06-24"

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* exact-fit memory allocation */
#define USE_EXACT_FIT 1

/* save extra data to load a mutable trie faster */
/* #undef USE_FAST_LOAD */

/* reducing trie size by introducing tail */
/* #undef USE_PREFIX_TRIE */

/* reducing trie size by pruning value nodes */
/* #undef USE_REDUCED_TRIE */

/* Version number of package */
#define VERSION "2014-06-24"

/* Define for Solaris 2.5.1 so the uint8_t typedef from <sys/synch.h>,
   <pthread.h>, or <semaphore.h> is not used. If the typedef were allowed, the
   #define below would cause a syntax error. */
/* #undef _UINT8_T */

/* Define to `unsigned int' if <sys/types.h> does not define. */
/* #undef size_t */

/* Define to `int' if <sys/types.h> does not define. */
/* #undef ssize_t */

/* Define to the type of an unsigned integer type of width exactly 16 bits if
   such a type exists and the standard includes do not define it. */
/* #undef uint16_t */

/* Define to the type of an unsigned integer type of width exactly 8 bits if
   such a type exists and the standard includes do not define it. */
/* #undef uint8_t */

// #define USE_PERSISTENT_CEDAR true