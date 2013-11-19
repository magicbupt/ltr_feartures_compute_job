CC = g++ 
INCLUDE =
BIN = 
LIB =
 
CFLAGS = -D_GNU_SOURCE -Wall -g  

LDFLAGS = 
TARGET = map2hashmap
		
all: $(TARGET)
		
$(TARGET):map2staticmap.o
	$(CC) -o $@ $^ $(LIB)
		
%.o : %.cpp
	$(CC) -c $(CFLAGS) $< $(INCLUDE)
		
clean :
	$(RM) *.o
	$(RM) map2hashmap 
