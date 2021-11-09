package com.springboot.casandra.betterreadsdataloader;

import com.springboot.casandra.betterreadsdataloader.model.AuthorBooks;
import com.springboot.casandra.betterreadsdataloader.model.Book;
import com.springboot.casandra.betterreadsdataloader.repository.AuthorBooksRepository;
import com.springboot.casandra.betterreadsdataloader.repository.BookRepository;
import com.springboot.casandra.connection.DataStaxAstraProperties;
import com.springboot.casandra.betterreadsdataloader.model.Author;
import com.springboot.casandra.betterreadsdataloader.repository.AuthorRepository;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

    @Autowired
    private AuthorRepository authorRepository;

    @Autowired
    private BookRepository bookRepository;

    @Autowired
    private AuthorBooksRepository authorBooksRepository;

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    private List<AuthorBooks> authorBooksList = new ArrayList<>();

    public static void main(String[] args) {
        SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties){
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return cqlSessionBuilder -> cqlSessionBuilder.withCloudSecureConnectBundle(bundle);
    }

    @PostConstruct
    public void start(){
      /*  Author author = new Author();
        author.setId("new id");
        author.setName("new name");
        author.setPersonalName("new personal name");

        authorRepository.save(author); */



        System.out.println("Before calling initAuthors...");
        initAuthors();
        System.out.println("After calling initAuthors..");

        System.out.println("Before calling initWorks...");
        initWorks();
        System.out.println("After calling initWorks..");

        // SELECT * FROM book_by_id WHERE author_ids contains 'OWWW' ALLOW FILTERING;
        initAuthorBooks();
    }

    private void initAuthorBooks(){

        authorBooksList.stream().forEach(authorBooks -> {
            if(authorBooks.getId()!=null) {
                List<Book> bookList = bookRepository.findAllByAuthorIds(authorBooks.getId());
                System.out.println(authorBooks.getId()+" Size::"+bookList.size());
                  List<String> bookIds = new ArrayList<>();
                List<String> bookNames = new ArrayList<>();
                List<String> coverIds = new ArrayList<>();

                for (Book book : bookList) {
                    bookIds.add(book.getId());
                    bookNames.add(book.getName());
                    authorBooks.setCoverIds(book.getCoverIds());
                }

                if(bookIds.size()>0)
                    authorBooks.setBookIds(bookIds);
                if(bookNames.size()>0)
                    authorBooks.setBookNames(bookNames);
              //  if(coverIds.size()>0)
                //    authorBooks.setCoverIds(coverIds);

                authorBooksRepository.save(authorBooks);
            }
        });
    }

    private void initAuthors(){
        Path path = Paths.get(authorDumpLocation);
        try(Stream<String> lines = Files.lines(path)){

            lines.forEach(line -> {
                try {
                    // Step 1: Read and Parse the line
                    String jsonString = line.substring(line.indexOf("{"));
                    JSONObject jsonObject = new JSONObject(jsonString);

                    // Step 2: Construct the author object
                    Author author = new Author();
                    author.setId(jsonObject.optString("key").replace("/authors/",""));
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));

                    final AuthorBooks authorBooks = new AuthorBooks();
                    authorBooks.setId(author.getId());
                    authorBooks.setName(author.getName());
                    authorBooksList.add(authorBooks);

                    // Step 3: Save the author object
                    authorRepository.save(author);
                    }
                catch (JSONException e){
                    e.printStackTrace();
                }
            });

        } catch (IOException e){
            e.printStackTrace();
        }
    }

    private void initWorks() {
        Path path = Paths.get(worksDumpLocation);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

        // Reading the lines from test-works.txt
        try(Stream<String> lines = Files.lines(path)){
            // Read each line
            lines.forEach(line -> {
                // Get the string starting from '{' till end of the line
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    // convert json string to json object
                    JSONObject jsonObject = new JSONObject(jsonString);

                    Book book = new Book();
                    // Id
                    book.setId(jsonObject.optString("key").replace("/works/",""));
                    // name
                    book.setName(jsonObject.optString("title"));
                    // description
                    JSONObject descriptionObj = jsonObject.optJSONObject("description");
                    if(descriptionObj!=null){
                        book.setDescription(descriptionObj.optString("value"));
                    }

                    // published date
                    JSONObject dateObj = jsonObject.getJSONObject("created");
                    if(dateObj!=null){
                        book.setPublishedDate(LocalDate.parse(dateObj.getString("value"), dateTimeFormatter));
                    }

                    // cover id's
                    JSONArray coverArray = jsonObject.optJSONArray("covers");
                    if(coverArray!=null) {
                        List<String> coverIds = new ArrayList<>();
                        for(int i=0;i<coverArray.length();i++){
                            coverIds.add(coverArray.getString(i));
                        }
                        book.setCoverIds(coverIds);
                    }

                    // author id's
                    JSONArray authorArray = jsonObject.getJSONArray("authors");
                    if(authorArray!=null) {
                        List<String> authorIds = new ArrayList<>();
                        for(int i=0;i<authorArray.length();i++){
                            String authorId = authorArray.getJSONObject(i)
                                                    .getJSONObject("author")
                                                    .getString("key")
                                                    .replace("/authors/","");
                            authorIds.add(authorId);
                        }
                        book.setAuthorIds(authorIds);
                    }

                    // author names
                    List<String> authorNames =
                        book.getAuthorIds().stream()
                                .map(authorId -> authorRepository.findById(authorId))
                                    .map(optionalAuthor -> {
                                        if(!optionalAuthor.isPresent())
                                            return "N/A";
                                        return optionalAuthor.get().getName();
                                    }).collect(Collectors.toList());
                    book.setAuthorNames(authorNames);

                    bookRepository.save(book);
                }catch (Exception e){
                    e.printStackTrace();
                }
            });
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}
