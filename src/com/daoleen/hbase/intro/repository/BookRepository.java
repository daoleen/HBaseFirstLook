package com.daoleen.hbase.intro.repository;

import java.io.IOException;
import java.util.List;

import com.daoleen.hbase.intro.domain.Book;

public interface BookRepository {
	public void save(Book book) throws IOException;
	public Book read(String id) throws IOException;
	public List<Book> readAll() throws IOException;
	public void delete(String id) throws IOException;
	public List<Book> filterCBooks() throws IOException;
	public List<Book> filterByAuthorLastName() throws IOException;
}
