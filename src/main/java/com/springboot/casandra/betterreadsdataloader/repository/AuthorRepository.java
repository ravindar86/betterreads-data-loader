package com.springboot.casandra.betterreadsdataloader.repository;

import com.springboot.casandra.betterreadsdataloader.model.Author;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AuthorRepository extends CassandraRepository<Author,String> {
}
