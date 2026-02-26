/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package PurchaseOnlineRestService;

/**
 *
 * @author Dulya
 */
public class LogXmlBuilder {

    private final StringBuilder sb = new StringBuilder();

    // t=1 → String
    public LogXmlBuilder addString(String fieldName, String value) {
        sb.append("<d t=1 f=").append(fieldName).append(">")
                .append(escapeXml(value))
                .append("</d>");
        return this;
    }

    // t=2 → Date (แปลงเป็น พ.ศ. รูปแบบ d/M/yyyy)
    public LogXmlBuilder addDate(String fieldName, java.sql.Date date) {
        if (date == null) {
            sb.append("<d t=2 f=").append(fieldName).append("></d>");
            return this;
        }
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTime(date);
        int d = cal.get(java.util.Calendar.DAY_OF_MONTH);
        int m = cal.get(java.util.Calendar.MONTH) + 1;
        int y = cal.get(java.util.Calendar.YEAR);
        sb.append("<d t=2 f=").append(fieldName).append(">")
                .append(d).append("/").append(m).append("/").append(y)
                .append("</d>");
        return this;
    }

    // t=4 → Boolean
    public LogXmlBuilder addBoolean(String fieldName, boolean value) {
        sb.append("<d t=4 f=").append(fieldName).append(">")
                .append(value ? "True" : "False")
                .append("</d>");
        return this;
    }

    // t=5 → Integer / Enum
    public LogXmlBuilder addInt(String fieldName, int value) {
        sb.append("<d t=5 f=").append(fieldName).append(">")
                .append(value)
                .append("</d>");
        return this;
    }

    // สร้าง XML สมบูรณ์
    public String build() {
        return "<?xml version=\"1.0\" encoding=\"utf-8\"?><top>"
                + sb.toString()
                + "</top>";
    }

    // escape อักขระพิเศษ XML
    private String escapeXml(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
    }
}
