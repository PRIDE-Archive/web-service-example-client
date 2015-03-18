package uk.ac.ebi.pride.archive.web.service.example;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;
import uk.ac.ebi.pride.archive.web.service.model.assay.AssayDetail;
import uk.ac.ebi.pride.archive.web.service.model.assay.AssayDetailList;
import uk.ac.ebi.pride.archive.web.service.model.file.FileDetail;
import uk.ac.ebi.pride.archive.web.service.model.file.FileDetailList;
import uk.ac.ebi.pride.archive.web.service.model.project.ProjectDetail;
import uk.ac.ebi.pride.archive.web.service.model.project.ProjectSummary;
import uk.ac.ebi.pride.archive.web.service.model.project.ProjectSummaryList;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

/**
 * Example of a Java Client to consume PRIDE Archive RESTful web services.
 *
 * This code is provided to demonstrate a possible use of the web service
 * and the Java object model which is also made available by the PRIDE team.
 * It's far from production quality and does not make any claim to correctness
 * or completeness. It should only be used as a guideline/example.
 *
 * @author florian@ebi.ac.uk.
 */
@SuppressWarnings("unused")
public class WsClient {

    // use a Jackson JSON object mapper to map the retrieved JSON String
    // onto the Java objects of the web service data model.
    private ObjectMapper objectMapper;

    /**
     * An example client that uses the PRIDE Archive web service to query for
     * and retrieve data for public datasets in PRIDE.
     */
    public WsClient() {
        objectMapper = new ObjectMapper();
        // In case the used Java object model does not fully match the
        // returned JSON data model, the data mapper could produce errors.
        // In order to avoid this we can instruct the mapper to ignore
        // unrecognised fields. This may be a good practise to make the
        // code more resilient against small model changes and the client
        // code can concentrate on the data it really needs, which makes
        // the code more readable and maintainable.
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * Method to retrieve a list of Files (including some metadata)
     * for a given assay accession.
     *
     * @param assayAccession the accession of the PRIDE assay.
     * @return A FileDetailList with the details of the files associated to
     *        the assay accession.
     * @throws Exception
     */
    public FileDetailList getFilesForAssay(String assayAccession) throws Exception {
        URL url = new URL("http://www.ebi.ac.uk/pride/ws/archive/file/list/assay/" + assayAccession);
        String content = queryService(url);
        return objectMapper.readValue(content, FileDetailList.class);
    }

    /**
     * Method to retrieve a list of Files (including some metadata)
     * for a given project accession.
     *
     * @param projectAccession the accession of the PRIDE project.
     * @return A FileDetailList with the details of the files associated to
     *        the project accession.
     * @throws Exception
     */
    public FileDetailList getFilesForProject(String projectAccession) throws Exception {
        // valid project accession have to start with 'PRD' for legacy PRIDE datasets or 'PXD' for ProteomeXchange datasets
        if (projectAccession.startsWith("PRD") || projectAccession.startsWith("PXD")) {
            URL url = new URL("http://www.ebi.ac.uk/pride/ws/archive/file/list/project/" + projectAccession);
            String content = queryService(url);
            return objectMapper.readValue(content, FileDetailList.class);
        } else {
            return null;
        }
    }

    /**
     * Method to retrieve details for a specific project/dataset.
     *
     * @param projectAccession the accession of the PRIDE project.
     * @return the ProjectDetail object containing the project's details.
     * @throws Exception
     */
    public ProjectDetail getProjectDetails(String projectAccession) throws Exception {
        URL url = new URL("http://www.ebi.ac.uk/pride/ws/archive/project/" + projectAccession);
        String content = queryService(url);
        return objectMapper.readValue(content, ProjectDetail.class);
    }

    /**
     * Method to retrieve details for a specific assay.
     *
     * @param assayAccession the accession of the PRIDE assay.
     * @return the AssayDetail object containing the assay's details.
     * @throws Exception
     */
    public AssayDetail getAssayDetails(String assayAccession) throws Exception {
        URL url = new URL("http://www.ebi.ac.uk/pride/ws/archive/assay/" + assayAccession);
        String content = queryService(url);
        return objectMapper.readValue(content, AssayDetail.class);
    }

    /**
     * Method to retrieve details on all assays belonging to a specific project/dataset.
     *
     * @param projectAccession the accession of the PRIDE project.
     * @return a AssayDetailList with the details for all the assays of the project.
     * @throws Exception
     */
    public AssayDetailList getAssayDetailForProject(String projectAccession) throws Exception {
        URL url = new URL("http://www.ebi.ac.uk/pride/ws/archive/assay/list/project/" + projectAccession);
        String content = queryService(url);
        return objectMapper.readValue(content, AssayDetailList.class);
    }

    /**
     * Method to query the PRIDE Archive repository for projects/datasets which
     * are annotated with specific keywords.
     *
     * @param keywords a Set of keyword Strings to query for.
     * @param page the page of the result to retrieve.
     * @param show the number of results to retrieve per page.
     * @return a ProjectSummaryList with basic details of projects
     *         matching the query keywords.
     * @throws Exception
     */
    public ProjectSummaryList queryForProjects(Set<String> keywords, Integer page, Integer show) throws Exception {
        String query = createQuery(keywords, page, show);
        URL url = new URL("http://www.ebi.ac.uk/pride/ws/archive/project/list" + query);
        String content = queryService(url);
        return objectMapper.readValue(content, ProjectSummaryList.class);
     }

    /**
     * Method to count the projects/datasets which
     * are annotated with specific keywords.
     *
     * @param keywords a Set of keyword Strings to query for.
     * @return the number of projects matching the query keywords.
     * @throws Exception
     */
    public long countProjects(Set<String> keywords) throws Exception {
        String query = createQuery(keywords, null, null);
        URL url = new URL("http://www.ebi.ac.uk/pride/ws/archive/project/count" + query);
        String content = queryService(url);
        // since we use a count query the result should be parsed into a long
        return Long.parseLong(content);
    }

    /**
     * Method to create a query string from query keywords and paging parameters.
     * To be used for a project search.
     * @param keywords a Set of Strings to query for.
     * @param page the number of the result page to retrieve.
     * @param show the number of result records per result page.
     * @return The URL query parameters to add to the project list service.
     */
    private String createQuery(Set<String> keywords, Integer page, Integer show) {
        // first create the query for the search keywords
        StringBuilder sb = new StringBuilder();
        sb.append("?query=");
        Iterator<String> iter = keywords.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next());
            if (iter.hasNext()) {
                sb.append("%20");
            }
        }
        // then if present, add the paging parameters
        if (page != null) {
            sb.append("&page=").append(page);
        }
        if (show != null) {
            sb.append("&show=").append(show);
        }
        // return the completed query string
        return sb.toString();
    }

    /**
     * this method takes care of sending the request to the provided URL and
     * retrieving the response into a String.
     *
     * @param url the web service GET URL for the request.
     * @return a String containing the service response.
     * @throws Exception
     */
    private String queryService(URL url) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/json");

        if (conn.getResponseCode() != 200) {
            // this can be handled better, in order to allow the application to
            // report and react to connection/response issues
            // different error codes should be taken into account
            // and a general exception handling added
            throw new Exception("Failed : HTTP error code : " + conn.getResponseCode());
        }

        BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
        String currentLine;
        StringBuilder sb = new StringBuilder();
        while ((currentLine = br.readLine()) != null) {
            sb.append(currentLine);
        }
        conn.disconnect();

        return sb.toString();
    }


    public static void main(String[] args) throws Exception {

        // set up the available command line options
        Options options = new Options();
        options.addOption(new Option("h", "help", false, "print this message" ));
        options.addOption(new Option("c", "count", false, "count matching projects only" ));
        options.addOption(new Option("p", "page", true, "the result page to retrieve (0 based), default: 0"));
        options.addOption(new Option("s", "size", true, "the size of a result page, default: 5"));
        options.addOption(new Option("q", "query", true, "a query term (option can be given multiple times)" ));
        options.addOption(new Option("a", "assays", false, "for each project list the assays" ));
        options.addOption(new Option("f", "files", false, "for each project list the dataset files (may be a very long list)" ));

        // configurable variables that can be defined using command line arguments
        // we define sensible default values
        HashSet<String> queryTerms = new HashSet<String>();
        queryTerms.add("cancer");
        queryTerms.add("kidney");
        boolean countOnly = false; // don't count, but list results
        boolean listAssays = false; // don't list assays
        boolean listFiles = false; // don't list files
        int page = 0; // the first result page
        int size = 5; // limit the number of results to 5

        // process the command line arguments
        CommandLineParser parser = new BasicParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );

            if (line.getOptions().length < 1) {
                System.out.println("No options given executing default example. To see usage run with -h");
            }

            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "WsClient", options );
                System.exit(0);
            }

            if (line.hasOption("count")) {
                countOnly = true;
            }

            if (line.hasOption("query")) {
                queryTerms.clear(); // get rid of the defaults
                String[] tmp = line.getOptionValues("query");
                for (String s : tmp) {
                    // in case more than one query terms are given with one -q/query option,
                    // we have to split them and add them individually
                    Collections.addAll(queryTerms, s.split("\\s"));
                }
            }

            if (line.hasOption("page")) {
                page = Integer.parseInt(line.getOptionValue("page"));
            }
            if (line.hasOption("size")) {
                size = Integer.parseInt(line.getOptionValue("size"));
            }

            if (line.hasOption("assays")) {
                listAssays = true;
            }
            if (line.hasOption("files")) {
                listFiles = true;
            }

        } catch( ParseException exp ) {
            // oops, something went wrong
            System.err.println( "Parsing the command line failed.  Reason: " + exp.getMessage() );
            System.exit(-1);
        }



        // now that we have the required options, we can implement the actual client
        // using some example queries and printing parts of the results to stdout
        WsClient client = new WsClient();

        System.out.println("Search for datasets matching terms: " + queryTerms);

        if (countOnly) {
            long projectCount = client.countProjects(queryTerms);
            System.out.println("Number of projects matching query terms: " + projectCount);
        } else {
            ProjectSummaryList projectList = client.queryForProjects(queryTerms, page, size);

            if (projectList == null || projectList.getList().isEmpty()) {
                // if we could not find records for the given query terms,
                // we print a quick note and exit
                System.out.println("No records matching the query parameters: " + queryTerms);
                System.exit(0);
            }

            // for each project that was returned by the service we print a quick summary
            // and possibly assay and file lists
            for (ProjectSummary projectSummary : projectList.getList()) {
                System.out.println();
                System.out.println("Project: " + projectSummary.getAccession());
                System.out.println("\tTitle:\t\t" + projectSummary.getTitle());
                System.out.println("\t# assays:\t" + projectSummary.getNumAssays());
                System.out.println("\tpublished:\t" + projectSummary.getPublicationDate());
                System.out.println("\tTags:\t\t" + projectSummary.getProjectTags());
                // list assays if requested
                if (listAssays) {
                    AssayDetailList assayList = client.getAssayDetailForProject(projectSummary.getAccession());
                    System.out.println("\tProject assay list");
                    for (AssayDetail assayDetail : assayList.getList()) {
                        System.out.print("\t\tAssay:" + assayDetail.getAssayAccession());
                        System.out.println(" (" + assayDetail.getTitle()+ ")");
                    }
                }
                // list files if requested
                if (listFiles) {
                    FileDetailList files = client.getFilesForProject(projectSummary.getAccession());
                    System.out.println("\tProject file list");
                    for (FileDetail file : files.getList()) {
                        System.out.println("\t\t" + file.getFileName());

                    }
                }
            }
        }

    }


}
