package uk.ac.ebi.pride.archive.web.service.example;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
        sb.append("?q=");
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

        // example client
        WsClient client = new WsClient();


        // Example to query for projects with keywords
        // We want to find projects on human cancer samples
        Set<String> queryTerms = new HashSet<String>(2);
        queryTerms.add("cancer");
        queryTerms.add("lung");
        queryTerms.add("human");
        System.out.println("Search for datasets matching terms: " + queryTerms);
        // for this demo we limit the response to the first 5 results
        ProjectSummaryList projectList = client.queryForProjects(queryTerms, null, 5);

        if (projectList == null || projectList.getList().isEmpty()) {
            // if we could not find records for the given query terms,
            // we print a quick note and exit
            System.out.println("No records matching the query parameters: " + queryTerms);
            System.exit(0);
        }

        for (ProjectSummary projectSummary : projectList.getList()) {
            System.out.println();
            System.out.println("Project: " + projectSummary.getAccession());
            System.out.println("\tTitle:\t\t" + projectSummary.getTitle());
            System.out.println("\t# assays:\t" + projectSummary.getNumAssays());
            System.out.println("\tpublished:\t" + projectSummary.getPublicationDate());
            System.out.println("\tTags:\t\t" + projectSummary.getProjectTags());
            System.out.println("\n");
        }

        // as example get the first project and print some more details on the dataset
        String projectAccession = projectList.getList().get(0).getAccession();
        System.out.println("Retrieving more details on project: " + projectAccession);

        ProjectDetail project = client.getProjectDetails(projectAccession);
        System.out.println();
        System.out.println("Details for project: " + project.getAccession());
        System.out.println("\tTitle:\t\t" + project.getTitle());
        System.out.println("\tdescription:" + project.getProjectDescription());
        System.out.println("\t# assays:\t" + project.getNumAssays());
        System.out.println("\tDOI:\t\t" + project.getDoi());
        System.out.println("\tTags:\t\t" + project.getProjectTags());

        AssayDetailList assayList = client.getAssayDetailForProject(projectAccession);
        System.out.println();
        System.out.println("  Assays for project " + projectAccession);
        for (AssayDetail assayDetail : assayList.getList()) {
            System.out.println("\tAccession:\t" + assayDetail.getAssayAccession());
            System.out.println("\tTitle:\t\t" + assayDetail.getTitle());
            System.out.println("\tLable:\t\t" + assayDetail.getShortLabel());
            FileDetailList assayFiles = client.getFilesForAssay(assayDetail.getAssayAccession());
            System.out.println("\tassociated files");
            for (FileDetail fileDetail : assayFiles.getList()) {
                System.out.println("\t\t\t" + fileDetail.getFileName());
            }
            System.out.println();
        }



        FileDetailList files = client.getFilesForProject(projectAccession);
        System.out.println();
        System.out.println("Files for project " + projectAccession);
        for (FileDetail file : files.getList()) {
            System.out.println("\t" + file.getFileName());

        }


    }


}
