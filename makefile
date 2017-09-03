CC = g++
CFLAGS = -Wall -g3 -O3
DEPS1 = rarray.hpp list.hpp bsearch.hpp structs.hpp Journal.hpp hash.hpp
DEPS2 = ventry.hpp jobscheduler.hpp infoarray.hpp city.h
OBJS = main.o rarray.o Journal.o ventry.o city.o jobscheduler.o infoarray.o structs.o

acid: $(OBJS)
	$(CC) $^ -o $@ -pthread

.cpp.o:
	$(CC) -c $< $(CFLAGS)

clean:
	rm -f *.o

main.o: $(DEPS1) $(DEPS2)
rarray.o: rarray.hpp
Journal.o: Journal.hpp
ventry.o: ventry.hpp
city.o: city.h
jobscheduler.o: jobscheduler.hpp
infoarray.o: infoarray.hpp bsearch.hpp
structs.o: structs.hpp
