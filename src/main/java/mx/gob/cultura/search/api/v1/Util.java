package mx.gob.cultura.search.api.v1;

import org.dizitart.no2.*;
import org.dizitart.no2.mapper.JacksonMapper;
import org.dizitart.no2.mapper.NitriteMapper;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;

public class Util {
    private static Nitrite db;
    public static final Nitrite getDB() {
        URL dbFile = Util.class.getClassLoader().getResource("data.db");
        URL dataFile = Util.class.getClassLoader().getResource("data.json");
        String dbPath = System.getProperty("java.io.tmpdir");

        if (null == db) {
            JSONArray data = new JSONArray();
            if (null != dataFile) {
                data = parseData();
            }

            try {
                dbPath = dbFile.toURI().getPath();
            } catch (URISyntaxException use) {
                use.printStackTrace();
            }

            db = Nitrite.builder()
                .compressed()
                .filePath(dbPath)
                .openOrCreate();

            if (data.length() > 0) {
                NitriteCollection coll = db.getCollection("objects");
                coll.createIndex("title", IndexOptions.indexOptions(IndexType.Fulltext, true));
                coll.createIndex("description", IndexOptions.indexOptions(IndexType.Fulltext, true));

                for (int i = 0; i < data.length(); i++) {
                    JSONObject o = data.getJSONObject(i);
                    NitriteMapper nitriteMapper = new JacksonMapper();
                    Document doc = nitriteMapper.parse(o.toString());
                    coll.update(doc, true);
                }
            }
        }

        return db;
    }

    private static JSONArray parseData() {
        InputStream inStr = Util.class.getClassLoader().getResourceAsStream("data.json");
        BufferedReader reader = new BufferedReader(new InputStreamReader(inStr));

        StringBuilder json = new StringBuilder();

        try {
            String line;
            while ((line = reader.readLine()) != null) {
                json.append(line);
            }
        } catch (IOException ioex) {
            ioex.printStackTrace();
        }

        return new JSONArray(json.toString());
    }
}
