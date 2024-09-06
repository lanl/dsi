-- Create the `publishers` table
CREATE TABLE publishers (
    publisher_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    address TEXT
);

-- Create the `authors` table
CREATE TABLE authors (
    author_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    birth_date DATE
);

-- Create the `books` table
CREATE TABLE books (
    book_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    publish_date DATE,
    publisher_id INTEGER,
    author_id INTEGER,
    FOREIGN KEY (publisher_id) REFERENCES publishers(publisher_id),
    FOREIGN KEY (author_id) REFERENCES authors(author_id)
);

-- Insert some sample data into `publishers`
INSERT INTO publishers (name, address) VALUES
('Penguin Random House', 'New York, NY'),
('HarperCollins', 'New York, NY');

-- Insert some sample data into `authors`
INSERT INTO authors (name, birth_date) VALUES
('J.K. Rowling', '1965-07-31'),
('George R.R. Martin', '1948-09-20');

-- Insert some sample data into `books`
INSERT INTO books (title, publish_date, publisher_id, author_id) VALUES
('Harry Potter and the Philosophers Stone', '1997-06-26', 1, 1),
('A Game of Thrones', '1996-08-06', 2, 2);
