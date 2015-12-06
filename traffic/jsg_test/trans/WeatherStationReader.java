import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.jetel.component.AbstractGenericTransform;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * 
 * @author kdyer
 *
 */
public class WeatherStationReader extends AbstractGenericTransform {

	private interface IParser<T> {
		T parse(String s);
	}

	private static class LongParser implements IParser<Long> {

		@Override
		public Long parse(String s) {
			if (s == null || s.trim().length() == 0) {
				return null;
			}
			return Long.parseLong(s);
		}
	}

	private static class StringParser implements IParser<String> {
		@Override
		public String parse(String s) {
			if (s == null || s.trim().length() == 0) {
				return null;
			}
			return s;
		}
	}

	private static class Mapping {
		private final int[] indices;
		private final IParser<?> parser;

		public Mapping(int[] indices, IParser<?> parser) {
			super();
			this.indices = indices;
			this.parser = parser;
		}

		@Override
		public String toString() {
			return "Mapping [indices=" + Arrays.toString(indices) + ", parser="
					+ parser + "]";
		}

	}

	@Override
	public void execute() throws Exception {
		DataRecordMetadata outMetadata = getComponent().getOutMetadataArray()[0];
		String fileUrl = getProperties().getStringProperty("FileUrl");
		File file = getFile(fileUrl);
		Logger LOG = getLogger();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Reading input: " + fileUrl);
		}
		LongParser lParser = new LongParser();
		StringParser sParser = new StringParser();
		Map<String, Mapping> m = new HashMap<String, Mapping>();
		m.put("STNIDNUM", new Mapping(new int[] { 0, 8 }, sParser));
		m.put("RECTYPE", new Mapping(new int[] { 9, 11 }, sParser));
		m.put("COOPID", new Mapping(new int[] { 12, 18 }, sParser));
		m.put("CLIMDIV", new Mapping(new int[] { 19, 21 }, sParser));
		m.put("WBANID", new Mapping(new int[] { 22, 27 }, sParser));
		m.put("WMOID", new Mapping(new int[] { 28, 33 }, sParser));
		m.put("FAAID", new Mapping(new int[] { 34, 38 }, sParser));
		m.put("NWSID", new Mapping(new int[] { 39, 44 }, sParser));
		m.put("ICAOID", new Mapping(new int[] { 45, 49 }, sParser));
		m.put("COUNTRYNAME", new Mapping(new int[] { 50, 69 }, sParser));
		m.put("STATEPROV", new Mapping(new int[] { 71, 73 }, sParser));
		m.put("COUNTY", new Mapping(new int[] { 74, 104 }, sParser));
		m.put("TIME_ZONE", new Mapping(new int[] { 105, 110 }, sParser));
		m.put("COOPNAME", new Mapping(new int[] { 111, 141 }, sParser));
		m.put("WBANNAME", new Mapping(new int[] { 142, 172 }, sParser));
		m.put("BEGINDATE", new Mapping(new int[] { 173, 181 }, sParser));
		m.put("ENDDATE", new Mapping(new int[] { 182, 190 }, sParser));
		m.put("LATDIR", new Mapping(new int[] { 191 }, sParser));
		m.put("LAT_D", new Mapping(new int[] { 192, 194 }, lParser));
		m.put("LAT_M", new Mapping(new int[] { 195, 197 }, lParser));
		m.put("LAT_S", new Mapping(new int[] { 198, 200 }, lParser));
		m.put("LONDIR", new Mapping(new int[] { 201 }, sParser));
		m.put("LON_D", new Mapping(new int[] { 202, 205 }, lParser));
		m.put("LON_M", new Mapping(new int[] { 206, 208 }, lParser));
		m.put("LON_S", new Mapping(new int[] { 209, 211 }, lParser));
		m.put("LATLONPREC", new Mapping(new int[] { 212, 214 }, lParser));
		m.put("EL_GROUND", new Mapping(new int[] { 215, 221 }, lParser));
		m.put("EL_OTHER", new Mapping(new int[] { 222, 228 }, lParser));
		m.put("ELEVOTHERTYPE", new Mapping(new int[] { 229, 231 }, lParser));
		m.put("RELOC", new Mapping(new int[] { 232, 243 }, sParser));
		m.put("STNTYPE", new Mapping(new int[] { 244, 294 }, sParser));
		if (LOG.isInfoEnabled()) {
			LOG.info("mappings= " + m);
		}
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String str = null;
			int i = 1;
			while ((str = reader.readLine()) != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Processing row= " + i + " str= " + str);
				}
				boolean writeRecord = true;
				DataRecord record = DataRecordFactory.newRecord(outMetadata);
				for (Entry<String, Mapping> e : m.entrySet()) {
					String fieldId = e.getKey();
					Mapping mapping = e.getValue();
					//
					int[] indices = mapping.indices;
					String s = indices.length > 1 ? str.substring(indices[0],
							indices[1]) : String
							.valueOf(str.charAt(indices[0]));
					s = s.trim();
					if ("COOPID".equals(fieldId)) {
						if (!s.startsWith("04")) {
							writeRecord = false;
							break;
						}
					}
					try {
						record.getField(fieldId).setValue(
								mapping.parser.parse(s));
					} catch (Exception e1) {
						throw new JetelRuntimeException(
								"Error while parsing field " + fieldId
										+ " with indices= "
										+ Arrays.toString(indices));
					}
				}
				//
				if (writeRecord) {
					writeRecordToPort(0, record);
				}
				i++;
			}
		} catch (IOException e) {
			throw new JetelRuntimeException(e);
		}
	}

	protected void writeRecord(DataRecord dataRecord) {
		writeRecordToPort(0, dataRecord);
	}
}
