package com.daoleen.hbase.intro.repository.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.daoleen.hbase.intro.domain.Author;
import com.daoleen.hbase.intro.domain.Book;
import com.daoleen.hbase.intro.repository.BookRepository;

public class BookRepositoryImpl implements BookRepository {

	// TODO: place to config
	private final static byte[] TABLE = Bytes.toBytes("books");
	private final static byte[] BOOK_CF = Bytes.toBytes("b");
	private final static byte[] AUTHOR_CF = Bytes.toBytes("au");

	private HTablePool pool;

	public BookRepositoryImpl(HTablePool pool) {
		this.pool = pool;
	}

	@Override
	public void save(Book book) throws IOException {
		HTableInterface books = pool.getTable(TABLE);
		Put put = new Put(Bytes.toBytes(book.getId()));
		put.add(BOOK_CF, Bytes.toBytes("name"), Bytes.toBytes(book.getName()));
		//put.add(BOOK_CF, Bytes.toBytes("year"), Bytes.toBytes(book.getYear()));
		put.add(BOOK_CF, Bytes.toBytes("year"), Bytes.toBytes(String.valueOf(book.getYear())));

		if (book.getAuthor().getFirstName() != null) {
			put.add(AUTHOR_CF, Bytes.toBytes("fn"),
					Bytes.toBytes(book.getAuthor().getFirstName()));
		}

		if (book.getAuthor().getLastName() != null) {
			put.add(AUTHOR_CF, Bytes.toBytes("ln"),
					Bytes.toBytes(book.getAuthor().getLastName()));
		}

		books.put(put);
		books.close();
	}

	@Override
	public Book read(String id) throws IOException {
		HTableInterface books = pool.getTable(TABLE);
		Get get = new Get(Bytes.toBytes(id));
		get.addFamily(BOOK_CF);
		get.addFamily(AUTHOR_CF);
		Result result = books.get(get);
		Book book = createBook(result);
		books.close();
		return book;
	}

	@Override
	public List<Book> readAll() throws IOException {
		HTableInterface books = pool.getTable(TABLE);
		Scan scan = new Scan();
		scan.addFamily(BOOK_CF);
		scan.addFamily(AUTHOR_CF);
		ResultScanner results = books.getScanner(scan);
		List<Book> bks = new ArrayList<Book>();

		for (Result r : results) {
			bks.add(createBook(r));
		}

		books.close();
		return bks;
	}

	@Override
	public void delete(String id) throws IOException {
		HTableInterface books = pool.getTable(TABLE);
		Delete delete = new Delete(Bytes.toBytes(id));
		books.delete(delete);
		books.close();
	}

	@Override
	public List<Book> filterCBooks() throws IOException {
		Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("C.*"));
		HTableInterface books = pool.getTable(TABLE);
		Scan scan = new Scan();
		scan.addFamily(BOOK_CF);
		scan.addFamily(AUTHOR_CF);
		scan.setFilter(filter);
		ResultScanner results = books.getScanner(scan);
		List<Book> bks = new ArrayList<Book>();

		for (Result r : results) {
			bks.add(createBook(r));
		}
		
		books.close();
		
		return bks;
	}
	
	@Override
	public List<Book> filterByAuthorLastName() throws IOException {
		// Find books whose author's last name is 'Kozlov'
		// I need to use a VALUE filter!
		Filter filter = new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("Kozlov")));
		HTableInterface books = pool.getTable(TABLE);
		Scan scan = new Scan();
		scan.addFamily(BOOK_CF);
		scan.addFamily(AUTHOR_CF);
		scan.addColumn(AUTHOR_CF, Bytes.toBytes("ln"));	// adds column to scan
		scan.setFilter(filter);
		ResultScanner results = books.getScanner(scan);
		List<Book> bks = new ArrayList<Book>();

		for (Result r : results) {
			bks.add(createBook(r));
		}
		
		books.close();
		
		return bks;
	}
	
	private Book createBook(Result result) {
		Book book = new Book();
		book.setId(Bytes.toString(result.getRow()));
		book.setName(Bytes.toString(result.getValue(BOOK_CF,
				Bytes.toBytes("name"))));

		byte[] yearBytes = result.getValue(BOOK_CF, Bytes.toBytes("year"));
		if (yearBytes != null) {
			book.setYear(Integer.parseInt(Bytes.toString(yearBytes)));
		}

		Author author = new Author(Bytes.toString(result.getValue(AUTHOR_CF,
				Bytes.toBytes("fn"))), Bytes.toString(result.getValue(
				AUTHOR_CF, Bytes.toBytes("ln"))));
		book.setAuthor(author);
		return book;
	}
}
