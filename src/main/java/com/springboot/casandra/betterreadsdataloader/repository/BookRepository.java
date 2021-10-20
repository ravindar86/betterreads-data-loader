package com.springboot.casandra.betterreadsdataloader.repository;

import com.springboot.casandra.betterreadsdataloader.model.Book;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BookRepository extends CassandraRepository<Book, String> {
}
