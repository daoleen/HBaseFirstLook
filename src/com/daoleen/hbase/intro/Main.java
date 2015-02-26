package com.daoleen.hbase.intro;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;

import com.daoleen.hbase.intro.domain.Author;
import com.daoleen.hbase.intro.domain.Book;
import com.daoleen.hbase.intro.repository.BookRepository;
import com.daoleen.hbase.intro.repository.impl.BookRepositoryImpl;

public class Main {
	private BookRepository bookRepository;
	
	public Main() {
		bookRepository = new BookRepositoryImpl(new HTablePool());
	}
	
	void showBooks() {
		try {
			List<Book> books = bookRepository.readAll();
			for(Book b : books) {
				System.out.println(b);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	void findBook() {
		try {
			Book book = bookRepository.read("Java_2");
			System.out.println(book);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	void createBook() {
		try {
			Book book = new Book();
			book.setId("Python");
			book.setName("pppython");
			book.setYear(2014);
			bookRepository.save(book);
			System.out.println("Saved....");
			showBooks();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	void updateBook() {
		try {
			Book book = new Book();
			book.setId("Python");
			book.setName("Just python updated");
			book.setYear(2014);
			book.setAuthor(new Author("Jack", "Doe"));
			bookRepository.save(book);
			System.out.println("Saved....");
			showBooks();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	void deleteBook() {
		try {
			bookRepository.delete("Python");
			showBooks();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	void showCBooks() {
		try {
			List<Book> books = bookRepository.filterCBooks();
			for(Book b : books) {
				System.out.println(b);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	void showKozlovBooks() {
		try {
			List<Book> books = bookRepository.filterByAuthorLastName();
			for(Book b : books) {
				System.out.println(b);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Main program = new Main();
		//program.showBooks();
		//program.findBook();
		//program.createBook();
		//program.updateBook();
		//program.deleteBook();
		program.showCBooks();
		//program.showKozlovBooks();
	}
}
