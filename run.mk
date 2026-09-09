run: msocket.h
	gcc -Wall -o user1 -I. -L. user1.c -lmsocket
	gcc -Wall -o user2 -I. -L. user2.c -lmsocket

clean:
	rm -f *.o *.a init user1 user2
