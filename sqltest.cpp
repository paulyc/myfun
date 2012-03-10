#include <cstdio>
#include <mysql.h>
#include <pthread.h>
#include <string>
#include <iostream>
#include <exception>
using namespace std;

pthread_mutex_t global_lock;

//template <typename T0, R>
//class functor2_r {
//};

class MysqlConn {
public:
	MysqlConn(const char *db) {
		_conn = mysql_init(NULL);
		mysql_real_connect(_conn, "localhost", "root", "root", db, 0, 0, 0);
		this->autocommit(false);
	}
	bool query(const char *q) {
		printf("%s\n", q);
		if (mysql_query(_conn, q) != 0) {
			throw string("that query failed");
		}
		return true;
	}
	bool commit() {
		return mysql_commit(_conn) == 0;
	}
	bool autocommit(bool enable) {
		return mysql_autocommit(_conn, enable ? 1 : 0) == 0;
	}
	int rowsAffected() {
		return mysql_affected_rows(_conn);
	}
	~MysqlConn() {
		mysql_close(_conn);
	}
private:
	MYSQL *_conn;
};

void process(int id, MysqlConn conn) {
	
}


void* read_thread1(void*) {
	MysqlConn *myConn = new MysqlConn("testdb");

	myConn->query("UPDATE queue_tok SET owner_token='abcdefg', update_time=NOW() WHERE tries=0 AND owner_token IS NULL ORDER BY create_time ASC LIMIT 1");

	int rowsAffected = myConn->rowsAffected();
	myConn->commit();
	if (rowsAffected > 0) {
		myConn->query("SELECT id FROM queue_tok WHERE token='abcdefg'");
		// i own id
	} else {
		// try, try again
	}

	delete myConn;
}

void* read_thread2(void* ) {
	MysqlConn *myConn = new MysqlConn("testdb");
	myConn->query("SELECT id FROM queue_tok WHERE tries=0 and owner_token IS NULL ORDER BY create_time ASC LIMIT 1");
	// pull id
	char buf[1024];
	sprintf(buf, "UPDATE queue_tok SET owner_token='ghijklmno', update_time=NOW() WHERE id=%d AND owner_token IS NULL", id);
	myConn->query(buf);
	int rowsAffected = myConn->rowsAffected();
	myConn->commit();
	// if update did 0 rows then retry
	if (rowsAffected > 0) {
		// i own id
	} else {
		// try, try again
	}
}

void* reap_thread(void*) {
	MysqlConn conn = new MysqlConn("testdb");
	while (running) {
		conn->query("SELECT id, owner_token FROM queue_tok WHERE owner_token IS NOT NULL AND update_time");
	}
	delete conn;
}


void* ins_thread(void*) {
	MysqlConn *myConn = new MysqlConn("testdb");

	for (int i = 0; i < 10 ; ++i) {
		myConn->query("INSERT INTO queue_tok(owner_token, create_time, update_time, tries) VALUES (NULL, NOW(), NULL, 0)");
	}
	myConn->commit();

	delete myConn;
}

void drop_table(MysqlConn *c) {
	c->query("DROP TABLE IF EXISTS queue_tok");
}

void create_table(MysqlConn *c) {
	c->query("CREATE TABLE queue_tok("
			 "id int NOT NULL AUTO_INCREMENT PRIMARY KEY, "
			 "owner_token varchar(32), create_time datetime, update_time datetime, tries int)");
}

void drop_create_table(MysqlConn *c) {
	drop_table(c);
	create_table(c);
}

void do_fun() {
	MysqlConn *conn = new MysqlConn("testdb");
	
	try {
		drop_create_table(conn);
	} catch (string e) {
		cerr << "toplevel exception handler caught " << e << endl;
	}
	
	delete conn;
}


int main() {
	if (mysql_library_init(0, NULL, NULL)) {
		fprintf(stderr, "could not initialize MySQL library\n");
		exit(1);
	}
	
	
	do_fun();
	
	
	mysql_library_end();
	return 0;
}
