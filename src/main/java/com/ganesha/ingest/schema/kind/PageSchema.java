package com.ganesha.ingest.schema.kind;

import com.ganesha.ingest.page.kind.ArticlePage;
import com.ganesha.ingest.schema.ParametrizedParsingSchema;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@NoArgsConstructor
@Log4j2
public class PageSchema extends ParametrizedParsingSchema<ArticlePage>
{
    public static final String TITLE_PATTERN = "titlePattern";
    public static final String PARAGRAPH_PATTERN = "paragraphPattern";
    public static final String TITLE_GROUP = "titleGroup";
    public static final String PARAGRAPH_GROUP = "paragraphGroup";

    @Override
    public ArticlePage convert(String body, int level, String calledUrl) {
        if (body == null || body.isEmpty()) {
            log.error("Empty body received for URL: {}", calledUrl);
            throw new IllegalArgumentException("Body cannot be null or empty");
        }

        String titleToStore = extractTitle(body, calledUrl);

        String paragraphPattern = params.get(PARAGRAPH_PATTERN);
        if (paragraphPattern == null) {
            log.error("Paragraph pattern not found in parameters for URL: {}", calledUrl);
            throw new IllegalStateException("Paragraph pattern not configured");
        }

        log.info("Using paragraph pattern: {}", paragraphPattern);
        
        if(log.isDebugEnabled()){
            //Log all paragraph elements to see their structure (for debugging)
            //this pattern may change per site
            Pattern pPattern = Pattern.compile("<p[^>]*>.*?</p>", Pattern.DOTALL);
            Matcher pMatcher = pPattern.matcher(body);
            while (pMatcher.find()) {
                log.debug("Found paragraph element: {}", pMatcher.group());
            }
        }
        //Log all paragraph elements to see their structure (for debugging)
        //Pattern pPattern = Pattern.compile("<p[^>]*>.*?</p>", Pattern.DOTALL);
        //Matcher pMatcher = pPattern.matcher(body);
        //while (pMatcher.find()) {
        //    log.debug("Found paragraph element: {}", pMatcher.group());
        //}

        int paragraphGroup = Integer.parseInt(params.get(PARAGRAPH_GROUP));
        List<String> paragraphs = extractPart(body, paragraphPattern, paragraphGroup);

        if (paragraphs.isEmpty()) {
            log.warn("No paragraphs found in body for URL: {} using pattern: {}", calledUrl, paragraphPattern);
            throw new IllegalStateException("No paragraphs found in the page");
        }

        log.info("Found {} paragraphs", paragraphs.size());

        return new ArticlePage(level, calledUrl, titleToStore, paragraphs);
    }

    private String extractTitle(String body, String calledUrl) {
        String titlePattern = params.get(TITLE_PATTERN);
        if (titlePattern == null) {
            log.error("Title pattern not found in parameters for URL: {}", calledUrl);
            return "N/A";
        }

        log.info("Using title pattern: {}", titlePattern);
        
        // Log potential title elements
        Pattern h1Pattern = Pattern.compile("<h1[^>]*>.*?</h1>", Pattern.DOTALL);
        Matcher h1Matcher = h1Pattern.matcher(body);
        while (h1Matcher.find()) {
            log.debug("Found h1 element: {}", h1Matcher.group());
        }

        int titleGroup = Integer.parseInt(params.get(TITLE_GROUP));
        List<String> titles = extractPart(body, titlePattern, titleGroup);

        if (titles.isEmpty()) {
            log.warn("No title found in body for URL: {} using pattern: {}", calledUrl, titlePattern);
            return "N/A";
        }

        String title = titles.get(0);
        log.info("Title: {}", title);
        return title;
    }

    private List<String> extractPart(String body, String elementPattern, int elementGroup) {
        List<String> allMatches = new ArrayList<>();
        Pattern pattern = Pattern.compile(elementPattern, Pattern.DOTALL);
        Matcher m = pattern.matcher(body);
        while (m.find()) {
            allMatches.add(m.group());
        }

        if (allMatches.isEmpty()) {
            log.warn("No matches found for pattern: {}", elementPattern);
            return new ArrayList<>();
        }

        log.debug("Found {} raw matches for pattern: {}", allMatches.size(), elementPattern);
        if (!allMatches.isEmpty()) {
            log.debug("First match: {}", allMatches.get(0));
        }

        List<String> extractedElements = allMatches.stream()
            .map(e -> {
                Matcher elementMatcher = pattern.matcher(e);
                if (elementMatcher.matches()) {
                    try {
                        String group = elementMatcher.group(elementGroup);
                        log.debug("Extracted group {}: {}", elementGroup, group);
                        return group;
                    } catch (IndexOutOfBoundsException ex) {
                        log.error("Invalid group index {} for pattern: {}", elementGroup, elementPattern);
                        return null;
                    }
                }
                return null;
            })
            .filter(e -> e != null)
            .toList();

        if (extractedElements.isEmpty()) {
            log.warn("No valid matches found after group extraction for pattern: {}", elementPattern);
        }

        return extractedElements;
    }

    @Override
    public String getSchemaId() {
        return this.getClass().getSimpleName();
    }
}
