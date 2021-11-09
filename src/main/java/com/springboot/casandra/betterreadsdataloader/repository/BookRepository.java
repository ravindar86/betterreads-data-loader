package com.springboot.casandra.betterreadsdataloader.repository;

import com.springboot.casandra.betterreadsdataloader.model.Book;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface BookRepository extends CassandraRepository<Book, String> {

    @Query("SELECT * FROM book_by_id WHERE author_ids contains ?0 ALLOW FILTERING")
    List<Book> findAllByAuthorIds(String id);
}
