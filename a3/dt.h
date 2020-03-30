#ifndef DT_H
#define DT_H

// define bool if not defined
#ifndef bool 
	typedef short bool;
#define true 1
#define false 0
#endif

#define TRUE true
#define FALSE false

#define PACKED_STRUCT __attribute__((__packed__))

#endif // DT_H
