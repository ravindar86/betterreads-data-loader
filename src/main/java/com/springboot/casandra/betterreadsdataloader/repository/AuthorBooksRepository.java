package com.springboot.casandra.betterreadsdataloader.repository;

import com.springboot.casandra.betterreadsdataloader.model.Author;
import com.springboot.casandra.betterreadsdataloader.model.AuthorBooks;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AuthorBooksRepository extends CassandraRepository<AuthorBooks,String> {
}
