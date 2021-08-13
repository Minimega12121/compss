package es.bsc.compss.http.master;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.CannotLoadException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.implementations.HTTPImplementation;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.util.RequestDispatcher;
import es.bsc.compss.util.RequestQueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class HTTPCaller extends RequestDispatcher<HTTPJob> {

    private static final String SUBMIT_ERROR = "Error calling HTTP Service";
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final String URL_PARAMETER_OPEN_TOKEN = "\\{\\{";
    private static final String URL_PARAMETER_CLOSE_TOKEN = "\\}\\}";


    public HTTPCaller(RequestQueue<HTTPJob> queue) {
        super(queue);
    }

    @Override
    public void processRequests() {
        while (true) {
            HTTPJob job = queue.dequeue();
            if (job == null) {
                break;
            }
            try {
                final TaskDescription taskDescription = job.getTaskParams();
                final Map<String, String> namedParameters = constructMapOfNamedParameters(taskDescription);

                HTTPImplementation httpImplementation = (HTTPImplementation) job.getImplementation();

                LOGGER.debug("Executing HTTP Request...");

                HTTPInstance httpInstance = job.getResourceNode();
                LOGGER.info("______ THIS IS A BASE URL FROM XML FILE ____:" + httpInstance.getConfig().getBaseUrl());

                Response httpResponse =
                    performHttpRequest(httpInstance.getConfig().getBaseUrl(), namedParameters, httpImplementation);

                // todo: beautify this and maybe check the empty string
                formatResponse(httpResponse, httpImplementation.getProduces());

                processResponse(job, httpResponse);

            } catch (Exception e) {
                final JobListener jobListener = job.getListener();
                jobListener.jobFailed(job, JobEndStatus.EXECUTION_FAILED, null);

                LOGGER.error(SUBMIT_ERROR, e);
            }
        }
    }

    private void extractPaths(JsonObject produces, Map<Object, String> map, String previousPath) {
        for (Object key : produces.keySet()) {
            String keyStr = (String) key;
            Object value = produces.get(keyStr);
            String path = "";

            if (value instanceof JsonObject) {
                path = previousPath + "," + keyStr;
                extractPaths((JsonObject) value, map, path);
            } else {
                String retKey = produces.getAsJsonPrimitive(keyStr).getAsString();
                map.put(formatKey(retKey), previousPath + "," + keyStr);
            }
        }
    }

    private String formatKey(String key) {
        return key.replaceAll(URL_PARAMETER_OPEN_TOKEN, "\\$").replaceAll(URL_PARAMETER_CLOSE_TOKEN, "");
    }

    private void formatResponse(Response response, String produces) {

        if (produces == null || produces.equals(Constants.UNASSIGNED) || produces.equals("null")
            || produces.equals("#")) {
            JsonElement respBodyElem = JsonParser.parseString(response.getResponseBody().toString());
            JsonObject newBody = new JsonObject();
            newBody.add("$return_0", respBodyElem);
            response.setResponseBody(newBody);
            return;
        }

        JsonElement element = JsonParser.parseString(produces);
        JsonObject producesJSONObj = element.getAsJsonObject();
        Map<Object, String> paths = new HashMap();

        extractPaths(producesJSONObj, paths, "");

        JsonElement bodyJsonElement = JsonParser.parseString(response.getResponseBody().toString());
        JsonObject bodyJsonObject = bodyJsonElement.getAsJsonObject();

        JsonObject newBody = new JsonObject();

        for (Object key : paths.keySet()) {
            String keyString = paths.get(key).replaceFirst(",", "");
            newBody.add((String) key, extractValueFromJSON(bodyJsonObject, keyString));
        }

        response.setResponseBody(newBody);
    }

    private JsonElement extractValueFromJSON(JsonObject json, String keyString) {
        String[] keys = keyString.split(",");
        for (int i = 0; i < keys.length - 1; i++) {
            json = json.getAsJsonObject(keys[0]);
        }
        return json.get(keys[keys.length - 1]);
    }

    private String[] extractKeys(String produces) {
        // todo: beautify this
        String[] keys = produces.replaceAll("#", "").split(",");
        for (int i = 0; i < keys.length; i++) {
            String tmp = keys[i].trim();
            tmp = tmp.replaceAll(URL_PARAMETER_OPEN_TOKEN, "");
            tmp = tmp.replaceAll(URL_PARAMETER_CLOSE_TOKEN, "");
            keys[i] = tmp;
        }
        return keys;
    }

    private void processResponse(HTTPJob job, final Response response) {
        int httpResponseCode = response.getResponseCode();

        final JobListener jobListener = job.getListener();

        if (httpResponseCode >= 200 && httpResponseCode < 300) {
            LOGGER.debug("Correct HTTP response with response code " + httpResponseCode);
            job.setReturnValue(response.getResponseBody());
            jobListener.jobCompleted(job);
        } else {
            LOGGER.debug("Job failing due to wrong HTTP response with response code " + httpResponseCode);

            jobListener.jobFailed(job, JobEndStatus.EXECUTION_FAILED, null);
        }
    }

    private Response performHttpRequest(String baseUrl, final Map<String, String> namedParameters,
        final HTTPImplementation httpImplementation) throws IOException {

        final String fullUrl = baseUrl + httpImplementation.getBaseUrl();

        final String methodType = httpImplementation.getMethodType();
        final String parsedUrl = URLReplacer.replaceUrlParameters(fullUrl, namedParameters, URL_PARAMETER_OPEN_TOKEN,
            URL_PARAMETER_CLOSE_TOKEN);

        // nm:
        // todo: (do not) read file content
        String jsonPayload = httpImplementation.getJsonPayload();

        jsonPayload = URLReplacer.formatJsonPayload(jsonPayload, namedParameters, URL_PARAMETER_OPEN_TOKEN,
            URL_PARAMETER_CLOSE_TOKEN);

        return HTTPController.performRequestAndGetResponse(methodType, parsedUrl, jsonPayload);
    }

    private Map<String, String> constructMapOfNamedParameters(TaskDescription taskDescription)
        throws CannotLoadException {

        Map<String, String> namedParameters = new HashMap<>();

        for (Parameter par : taskDescription.getParameters()) {
            final Direction parameterDirection = par.getDirection();

            if (parameterDirection == Direction.IN || parameterDirection == Direction.IN_DELETE) {
                switch (par.getType()) {
                    case FILE_T:
                        if (par.getContentType() != null && par.getContentType().toUpperCase().equals("FILE")) {
                            DependencyParameter fileParam = (DependencyParameter) par;
                            String content = readFile(fileParam.getDataTarget());
                            namedParameters.put(par.getName(), content);
                        } else {
                            DependencyParameter dependencyParameter = (DependencyParameter) par;
                            final Object objectValue = getObjectValue(dependencyParameter);
                            addParameterToMapOfParameters(namedParameters, par, objectValue);
                        }
                        break;
                    case OBJECT_T:
                    case PSCO_T:
                    case EXTERNAL_PSCO_T:
                        // todo: is it the case only for pycompss?
                        // nm: check content type to know if it's a python object
                        DependencyParameter dependencyParameter = (DependencyParameter) par;
                        final Object objectValue = getObjectValue(dependencyParameter);
                        addParameterToMapOfParameters(namedParameters, par, objectValue);
                        break;
                    case STREAM_T:
                    case EXTERNAL_STREAM_T:
                        LOGGER.error("Error: HTTP CAN'T USE STREAMS AS PARAMETERS!");
                        // Skip
                        break;

                    case BINDING_OBJECT_T:
                        LOGGER.error("Error: HTTP CAN'T USE BINDING OBJECTS AS PARAMETERS!");
                        // Skip
                        break;

                    default:
                        // Basic or String
                        BasicTypeParameter basicTypeParameter = (BasicTypeParameter) par;
                        addParameterToMapOfParameters(namedParameters, par, basicTypeParameter.getValue());
                }
            } else if (parameterDirection == Direction.OUT) {
                LOGGER.debug("Out parameter of HTTPCaller: " + par);
            }
        }
        return namedParameters;
    }

    private void addParameterToMapOfParameters(Map<String, String> namedParameters, Parameter par, Object o) {
        String key = par.getName();
        if (key != null && !key.isEmpty()) {
            final String s = convertObjectToString(o);
            final String value = String.valueOf(s);
            namedParameters.put(key, value);
        }
    }

    private String convertObjectToString(Object o) {
        if (o instanceof Integer) {
            return Integer.toString((Integer) o);
        }
        if (o instanceof Float) {
            return String.valueOf(o);
        }
        return (String) o;
    }

    private Object getObjectValue(DependencyParameter dp) throws CannotLoadException {
        final DataAccessId dataAccessId = dp.getDataAccessId();
        final DataInstanceId dataInstanceId = ((RAccessId) dataAccessId).getReadDataInstance();
        String renaming = dataInstanceId.getRenaming();

        LogicalData logicalData = Comm.getData(renaming);

        if (!logicalData.isInMemory()) {
            logicalData.loadFromStorage();
        }
        return logicalData.getValue();
    }

    // todo: move it somewhere else
    private static String readFile(String fileName) {
        File f = new File(fileName);
        BufferedReader br = null;
        String contents = "";
        try {
            br = new BufferedReader(new FileReader(f));
            String line;
            while ((line = br.readLine()) != null) {
                contents = contents + line;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return contents;
    }
}
