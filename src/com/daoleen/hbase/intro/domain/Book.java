package com.daoleen.hbase.intro.domain;

public class Book {
	private String id;
	private String name;
	private int year;
	private Author author;
	
	public Book() {
		author = new Author();
	}
		

	@Override
	public String toString() {
		return "Book [id=" + id + ", name=" + name + ", year=" + year
				+ ", author=" + author + "]";
	}


	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public Author getAuthor() {
		return author;
	}

	public void setAuthor(Author author) {
		this.author = author;
	}
}
