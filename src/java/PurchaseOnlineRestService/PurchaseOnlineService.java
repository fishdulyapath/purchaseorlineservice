package PurchaseOnlineRestService;

import SMLWebService.Routine;
import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import org.apache.tomcat.util.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import utils._global;
import utils._routine;
import utils.myglobal;

@Path("purchaseonlineservice")
public class PurchaseOnlineService {

    private String _formatDate(Date date) {
        SimpleDateFormat _format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        return _format.format(date);
    }

    private static boolean isNotNull(String txt) {
        return txt != null && txt.trim().length() > 0 ? true : false;
    }

    @POST
    @Path("/additemtocart")
    public Response addItemToCart(
            String data
    ) throws Exception {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();

        __objResponse.put("success", false);
        try {
            String _guid_code = "";
            String _item_code = "";
            String _item_name = "";
            String _unit_code = "";
            String _barcode = "";
            String _qty = "";
            String _price = "";
            String _wh_code = "";
            String _shelf_code = "";
            String __cust_code = "";
            String __creator_code = "";
            String _stand_value = "";
            String _divide_value = "";
            String _ratio = "";
            if (data != null) {

                JSONArray objJSArr = new JSONArray(data);

                StringBuilder __query_builder = new StringBuilder();
                UUID uuid = UUID.randomUUID();
                String strGUID = uuid.toString();

                StringBuilder __deleteQuery = new StringBuilder();

                _routine __routine = new _routine();
                Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));

                for (int i = 0; i < objJSArr.length(); i++) {
                    JSONObject objJSData = objJSArr.getJSONObject(i);
                    __cust_code = objJSData.has("cust_code") ? objJSData.getString("cust_code") : "";
                    __creator_code = objJSData.has("emp_code") ? objJSData.getString("emp_code") : "";
                    _guid_code = objJSData.has("guid_code") ? objJSData.getString("guid_code") : "";
                    _item_code = objJSData.has("item_code") ? objJSData.getString("item_code") : "";
                    _item_name = objJSData.has("item_name") ? objJSData.getString("item_name") : "";
                    _unit_code = objJSData.has("unit_code") ? objJSData.getString("unit_code") : "";
                    _barcode = objJSData.has("barcode") ? objJSData.getString("barcode") : "";
                    _qty = objJSData.has("qty") ? objJSData.getString("qty") : "1";
                    _price = objJSData.has("price") ? objJSData.getString("price") : "0";
                    _wh_code = objJSData.has("wh_code") ? objJSData.getString("wh_code") : "";
                    _shelf_code = objJSData.has("shelf_code") ? objJSData.getString("shelf_code") : "";

                    _stand_value = objJSData.has("stand_value") ? objJSData.getString("stand_value") : "1";
                    _divide_value = objJSData.has("divide_value") ? objJSData.getString("divide_value") : "1";
                    _ratio = objJSData.has("ratio") ? objJSData.getString("ratio") : "1";

                    __deleteQuery.append("delete from ps_cart_order_temp where item_code = '" + _item_code + "' and unit_code = '" + _unit_code + "' and barcode = '" + _barcode + "' and cust_code = '" + __cust_code + "' and wh_code = '" + _wh_code + "' and shelf_code = '" + _shelf_code + "'   ;");

                    __query_builder.append("insert into ps_cart_order_temp (cust_code,guid_code,item_code,item_name,unit_code,barcode,qty,price,wh_code,shelf_code,creator_code,create_datetime,stand_value,divide_value,ratio) values ('" + __cust_code + "','" + _guid_code + "','" + _item_code + "','" + _item_name + "','" + _unit_code + "','" + _barcode + "'"
                            + ",'" + _qty + "','" + _price + "','" + _wh_code + "','" + _shelf_code + "','" + __creator_code + "','now()','" + _stand_value + "','" + _divide_value + "','" + _ratio + "');");

                }
                Statement __stmt2;
                System.out.println("__deleteQuery " + __deleteQuery.toString());
                System.out.println("__query_builder " + __query_builder);
                Statement __stmtdelete = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                __stmtdelete.executeUpdate(__deleteQuery.toString());
                __stmtdelete.close();

                __stmt2 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                __stmt2.executeUpdate(__query_builder.toString());
                __objResponse.put("msg", "success");
                __objResponse.put("success", true);
                __stmt2.close();
                __conn.close();

            } else {
                return Response.status(400).entity("{ERROR: Data is null}").build();
            }

        } catch (JSONException ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(__objResponse.toString(), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getcartitemlist")
    public Response getCartItemList(
            @QueryParam("cust_code") String strCustCode,
            @QueryParam("wh_code") String strWhCode) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));

//            String __strQUERY1 = "SELECT * from ps_cart_order_temp where cust_code = '" + strCustCode + "'";
            String __strQUERY1 = "SELECT \n"
                    + "    w.*\n"
                    + "\n"
                    + "FROM    ps_cart_order_temp w  WHERE \n"
                    + "    w.cust_code = '" + strCustCode + "' and w.wh_code = '" + strWhCode + "' "
                    + "ORDER BY \n"
                    + "    w.item_code;";
            System.out.println(__strQUERY1);
            Statement __stmt1;
            ResultSet __rs1;
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("cust_code", __rs1.getString("cust_code"));
                obj.put("guid_code", __rs1.getString("guid_code"));
                obj.put("item_code", __rs1.getString("item_code"));
                obj.put("item_name", __rs1.getString("item_name"));
                obj.put("unit_code", __rs1.getString("unit_code"));
                obj.put("barcode", __rs1.getString("barcode"));
                obj.put("qty", __rs1.getString("qty"));
                obj.put("price", __rs1.getString("price"));
                obj.put("wh_code", __rs1.getString("wh_code"));
                obj.put("shelf_code", __rs1.getString("shelf_code"));
                obj.put("creator_code", __rs1.getString("creator_code"));
                obj.put("create_datetime", __rs1.getString("create_datetime"));
                obj.put("stand_value", __rs1.getString("stand_value"));
                obj.put("divide_value", __rs1.getString("divide_value"));
                obj.put("ratio", __rs1.getString("ratio"));
                obj.put("balance_qty", "0");
                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();

            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getGroup")
    public Response getGroup(
            @QueryParam("search") String strSearch
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            String where = "";
            if (!strSearch.equals("")) {
                where += " code like '%" + strSearch + "%' or name_1 like '%" + strSearch + "%'";
            }
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));

            String __strQUERY1 = "SELECT code,name_1 FROM ic_group where 1=1 " + where + "  ORDER BY code";

            Statement __stmt1;
            ResultSet __rsHead;
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rsHead = __stmt1.executeQuery(__strQUERY1);
            JSONArray jsarr = new JSONArray();
            //JSONArray __jsonArr = _convertResultSetIntoJSON(__rs1);
            while (__rsHead.next()) {

                JSONObject obj = new JSONObject();

                obj.put("code", __rsHead.getString("code"));
                obj.put("name", __rsHead.getString("name_1"));

                jsarr.put(obj);
            }

            __objResponse.put("success", true);
            __objResponse.put("data", jsarr);
            __conn.close();
            __rsHead.close();
            __stmt1.close();
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getGroupSub")
    public Response getGroupSub(
            @QueryParam("search") String strSearch
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            String where = "";
            if (!strSearch.equals("")) {
                where += " code like '%" + strSearch + "%' or name_1 like '%" + strSearch + "%'";
            }
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));

            String __strQUERY1 = "SELECT code,name_1 FROM ic_group_sub where 1=1 " + where + "  ORDER BY code";

            Statement __stmt1;
            ResultSet __rsHead;
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rsHead = __stmt1.executeQuery(__strQUERY1);
            JSONArray jsarr = new JSONArray();
            //JSONArray __jsonArr = _convertResultSetIntoJSON(__rs1);
            while (__rsHead.next()) {

                JSONObject obj = new JSONObject();

                obj.put("code", __rsHead.getString("code"));
                obj.put("name", __rsHead.getString("name_1"));

                jsarr.put(obj);
            }

            __objResponse.put("success", true);
            __objResponse.put("data", jsarr);
            __conn.close();
            __rsHead.close();
            __stmt1.close();
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getGroupSub2")
    public Response getGroupSub2(
            @QueryParam("search") String strSearch
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            String where = "";
            if (!strSearch.equals("")) {
                where += " code like '%" + strSearch + "%' or name_1 like '%" + strSearch + "%'";
            }
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));

            String __strQUERY1 = "SELECT code,name_1 FROM ic_group_sub2 where 1=1 " + where + "  ORDER BY code";

            Statement __stmt1;
            ResultSet __rsHead;
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rsHead = __stmt1.executeQuery(__strQUERY1);
            JSONArray jsarr = new JSONArray();
            //JSONArray __jsonArr = _convertResultSetIntoJSON(__rs1);
            while (__rsHead.next()) {

                JSONObject obj = new JSONObject();

                obj.put("code", __rsHead.getString("code"));
                obj.put("name", __rsHead.getString("name_1"));

                jsarr.put(obj);
            }

            __objResponse.put("success", true);
            __objResponse.put("data", jsarr);
            __conn.close();
            __rsHead.close();
            __stmt1.close();
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getBrand")
    public Response getBrand(
            @QueryParam("search") String strSearch
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            String where = "";
            if (!strSearch.equals("")) {
                where += " code like '%" + strSearch + "%' or name_1 like '%" + strSearch + "%'";
            }

            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));

            String __strQUERY1 = "SELECT code,name_1 FROM ic_brand where 1=1 " + where + " ORDER BY code";

            Statement __stmt1;
            ResultSet __rsHead;
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rsHead = __stmt1.executeQuery(__strQUERY1);
            JSONArray jsarr = new JSONArray();
            //JSONArray __jsonArr = _convertResultSetIntoJSON(__rs1);
            while (__rsHead.next()) {

                JSONObject obj = new JSONObject();

                obj.put("code", __rsHead.getString("code"));
                obj.put("name", __rsHead.getString("name_1"));

                jsarr.put(obj);
            }

            __objResponse.put("success", true);
            __objResponse.put("data", jsarr);
            __conn.close();
            __rsHead.close();
            __stmt1.close();
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getCategory")
    public Response getCategory(
            @QueryParam("search") String strSearch
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            String where = "";
            if (!strSearch.equals("")) {
                where += " code like '%" + strSearch + "%' or name_1 like '%" + strSearch + "%'";
            }

            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));

            String __strQUERY1 = "SELECT code,name_1 FROM ic_category where 1=1 " + where + " ORDER BY code";

            Statement __stmt1;
            ResultSet __rsHead;
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rsHead = __stmt1.executeQuery(__strQUERY1);
            JSONArray jsarr = new JSONArray();
            //JSONArray __jsonArr = _convertResultSetIntoJSON(__rs1);
            while (__rsHead.next()) {

                JSONObject obj = new JSONObject();

                obj.put("code", __rsHead.getString("code"));
                obj.put("name", __rsHead.getString("name_1"));

                jsarr.put(obj);
            }

            __objResponse.put("success", true);
            __objResponse.put("data", jsarr);
            __conn.close();
            __rsHead.close();
            __stmt1.close();
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getDesign")
    public Response getDesign(
            @QueryParam("search") String strSearch
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            String where = "";
            if (!strSearch.equals("")) {
                where += " code like '%" + strSearch + "%' or name_1 like '%" + strSearch + "%'";
            }

            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));

            String __strQUERY1 = "SELECT code,name_1 FROM ic_design where 1=1 " + where + " ORDER BY code";

            Statement __stmt1;
            ResultSet __rsHead;
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rsHead = __stmt1.executeQuery(__strQUERY1);
            JSONArray jsarr = new JSONArray();
            //JSONArray __jsonArr = _convertResultSetIntoJSON(__rs1);
            while (__rsHead.next()) {

                JSONObject obj = new JSONObject();

                obj.put("code", __rsHead.getString("code"));
                obj.put("name", __rsHead.getString("name_1"));

                jsarr.put(obj);
            }

            __objResponse.put("success", true);
            __objResponse.put("data", jsarr);
            __conn.close();
            __rsHead.close();
            __stmt1.close();
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getModel")
    public Response getModel(
            @QueryParam("search") String strSearch
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            String where = "";
            if (!strSearch.equals("")) {
                where += " code like '%" + strSearch + "%' or name_1 like '%" + strSearch + "%'";
            }

            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));

            String __strQUERY1 = "SELECT code,name_1 FROM ic_model where 1=1 " + where + " ORDER BY code";

            Statement __stmt1;
            ResultSet __rsHead;
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rsHead = __stmt1.executeQuery(__strQUERY1);
            JSONArray jsarr = new JSONArray();
            //JSONArray __jsonArr = _convertResultSetIntoJSON(__rs1);
            while (__rsHead.next()) {

                JSONObject obj = new JSONObject();

                obj.put("code", __rsHead.getString("code"));
                obj.put("name", __rsHead.getString("name_1"));

                jsarr.put(obj);
            }

            __objResponse.put("success", true);
            __objResponse.put("data", jsarr);
            __conn.close();
            __rsHead.close();
            __stmt1.close();
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getcartorder")
    public Response getCartConfirm(
            @QueryParam("cust_code") String strCustCode,
            @QueryParam("sale_type") String strSaleType) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));

            String __strQUERY1 = "SELECT *,coalesce((select tax_type from ic_inventory where code = item_code),0) as tax_type  from ps_cart_order_temp where cust_code = '" + strCustCode + "'";

            Statement __stmt1;
            ResultSet __rs1;
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);
            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("tax_type", __rs1.getString("tax_type"));
                obj.put("cust_code", __rs1.getString("cust_code"));
                obj.put("guid_code", __rs1.getString("guid_code"));
                obj.put("item_code", __rs1.getString("item_code"));
                obj.put("item_name", __rs1.getString("item_name"));
                obj.put("unit_code", __rs1.getString("unit_code"));
                obj.put("barcode", __rs1.getString("barcode"));
                obj.put("qty", __rs1.getString("qty"));
                obj.put("price", __rs1.getString("price"));
                obj.put("wh_code", __rs1.getString("wh_code"));
                obj.put("shelf_code", __rs1.getString("shelf_code"));
                obj.put("creator_code", __rs1.getString("creator_code"));
                obj.put("create_datetime", __rs1.getString("create_datetime"));
                obj.put("stand_value", __rs1.getString("stand_value"));
                obj.put("divide_value", __rs1.getString("divide_value"));
                obj.put("ratio", __rs1.getString("ratio"));
                obj.put("price_confirm", "0");
                String location = __rs1.getString("shelf_code");
                String year = "0";
                if (location != null && location.trim().matches("\\d{4}")) {
                    year = location.substring(0, 2);
                    String __strQUERYPrice = "SELECT "
                            + "  ic_code,unit_code, "
                            + "  CASE (RIGHT(EXTRACT(YEAR FROM NOW())::int::text, 2)::int - '" + year + "'::int) "
                            + "    WHEN 0 THEN '0' "
                            + "    WHEN 1 THEN coalesce(price_1,'0')  "
                            + "    WHEN 2 THEN coalesce(price_2,'0') "
                            + "    WHEN 3 THEN coalesce(price_3,'0') "
                            + "    WHEN 4 THEN coalesce(price_4,'0') "
                            + "    ELSE '0' "
                            + "  END AS price "
                            + "FROM ic_inventory_price_formula  "
                            + "WHERE ic_code = '" + __rs1.getString("item_code") + "'  and sale_type in (0," + strSaleType + ");";

                    Statement __stmtPrice = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    ResultSet __rsPrice = __stmtPrice.executeQuery(__strQUERYPrice);
                    System.out.println("__strQUERYPrice " + __strQUERYPrice);
                    while (__rsPrice.next()) {
                        obj.put("price_confirm", __rsPrice.getString("price"));
                    }

                    __rsPrice.close();
                    __stmtPrice.close();
                }

                if (obj.get("price_confirm").toString().equals("0")) {
                    JSONObject prices = getProductPriceLocal(__rs1.getString("item_code"), __rs1.getString("unit_code"), __rs1.getString("qty"), strCustCode, strSaleType);

                    JSONArray pricesArr = prices.optJSONArray("data");
                    if (pricesArr != null && pricesArr.length() > 0) {
                        JSONObject priceObj = pricesArr.optJSONObject(0);
                        obj.put("price_confirm", priceObj != null ? priceObj.optString("price", "0") : "0");
                    } else {
                        obj.put("price_confirm", "0");
                    }

                }

                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();

            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @POST
    @Path("/sendorder")
    public Response sendOrderSO(String data) {
        JSONObject response = new JSONObject();
        response.put("success", false);

        if (data == null || data.isEmpty()) {
            return badRequest("Request body is empty");
        }

        try {
            // --- 1. Parse Input ---
            JSONObject input = new JSONObject(data);

            String docNo = getStr(input, "doc_no", "");
            String custCode = getStr(input, "cust_code", "");
            String branchCode = getStr(input, "branch_code", "0000");
            String docDateStr = getStr(input, "doc_date", "");       // "yyyy-MM-dd"
            String docTime = getStr(input, "doc_time", "");       // "HH:mm"

            String empCode = getStr(input, "emp_code", "");
            String creatorCode = empCode;
            String remark = getStr(input, "remark", "");
            String saleType = getStr(input, "sale_type", "0");

            BigDecimal vatRate = new BigDecimal("7");
            BigDecimal totalValue = getBigDecimal(input, "total_value", BigDecimal.ZERO);
            BigDecimal totalExceptVat = getBigDecimal(input, "total_except_vat", BigDecimal.ZERO);
            BigDecimal totalAfterVat = getBigDecimal(input, "total_after_vat", BigDecimal.ZERO);
            BigDecimal totalAmount = getBigDecimal(input, "total_amount", BigDecimal.ZERO);

            // คำนวณ total_before_vat, total_vat_value (ใช้ BigDecimal แทน Float เพื่อความแม่นยำ)
            BigDecimal totalBeforeVat = totalAfterVat.multiply(new BigDecimal("100"))
                    .divide(new BigDecimal("107"), 4, RoundingMode.HALF_UP);
            BigDecimal totalVatValue = totalAfterVat.subtract(totalBeforeVat);

            // inquiry_type ตาม sale_type
            int inquiryType = "1".equals(saleType) ? 2 : 1;

            JSONArray items = input.has("items") ? input.getJSONArray("items") : new JSONArray();

            // --- 2. Validate ---
            if (custCode.isEmpty()) {
                return badRequest("Customer code is required");
            }
            if (docNo.isEmpty()) {
                return badRequest("Document number is required");
            }
            if (docDateStr.isEmpty()) {
                return badRequest("Document date is required");
            }
            if (items.length() == 0) {
                return badRequest("Items are required");
            }

            java.sql.Date docDate = java.sql.Date.valueOf(docDateStr); // yyyy-MM-dd

            // --- 3. Database ---
            String strProvider = "DEMO";
            String strDatabaseName = "demo1";

            _routine routine = new _routine();
            try (Connection conn = routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider))) {

                // Check duplicate (PK = doc_no + trans_flag)
                if (isDuplicateDoc(conn, docNo)) {
                    response.put("msg", "Duplicate Doc No.");
                    return Response.ok(response.toString(), MediaType.APPLICATION_JSON).build();
                }

                conn.setAutoCommit(false);
                try {
                    insertHeader(conn, docNo, docDate, docTime, custCode, branchCode,
                            vatRate, totalValue, totalBeforeVat, totalVatValue,
                            totalAfterVat, totalExceptVat, totalAmount,
                            inquiryType, creatorCode, empCode, remark);

                    insertDetails(conn, items, docNo, docDate, docTime, custCode);

                    clearCartTemp(conn, custCode);

                    conn.commit();
                    response.put("msg", "success");
                    response.put("success", true);
                } catch (Exception ex) {
                    conn.rollback();
                    throw ex;
                } finally {
                    conn.setAutoCommit(true);
                }
            }

        } catch (JSONException ex) {
            return badRequest("Invalid JSON: " + ex.getMessage());
        } catch (IllegalArgumentException ex) {
            return badRequest("Invalid data format: " + ex.getMessage());
        } catch (SQLException ex) {

            return Response.status(500).entity("{\"error\": \"Database error\"}").build();
        } catch (Exception ex) {

            return Response.status(500).entity("{\"error\": \"Internal server error\"}").build();
        }

        return Response.ok(response.toString(), MediaType.APPLICATION_JSON).build();
    }

// ============================================================
// Database Methods
// ============================================================
    private boolean isDuplicateDoc(Connection conn, String docNo) throws SQLException {
        // ic_trans PK = (doc_no, trans_flag)
        String sql = "SELECT 1 FROM ic_trans WHERE doc_no = ? AND trans_flag = 2 LIMIT 1";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, docNo);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private void insertHeader(Connection conn, String docNo, java.sql.Date docDate, String docTime,
            String custCode, String branchCode,
            BigDecimal vatRate, BigDecimal totalValue,
            BigDecimal totalBeforeVat, BigDecimal totalVatValue,
            BigDecimal totalAfterVat, BigDecimal totalExceptVat, BigDecimal totalAmount,
            int inquiryType, String creatorCode, String empCode,
            String remark) throws SQLException {

        String sql = "INSERT INTO ic_trans ("
                + "trans_type, trans_flag, " // smallint, smallint
                + "doc_date, doc_time, doc_no, " // date, varchar(5), varchar(25)
                + "inquiry_type, vat_type, " // smallint, smallint
                + "cust_code, branch_code, vat_rate, " // varchar(25), varchar(25), numeric
                + "total_value, total_before_vat, total_vat_value, " // numeric x3
                + "total_after_vat, total_except_vat, " // numeric x2
                + "total_amount, balance_amount, " // numeric x2
                + "user_request, approve_status, " // varchar(25), smallint
                + "doc_format_code, " // varchar(25)
                + "remark, creator_code, sale_code, " // varchar(255), varchar(255), varchar(25)
                + "create_datetime" // timestamp
                + ") VALUES ("
                + "1, 2, "
                + "?, ?, ?, "
                + "?, 1, "
                + "?, ?, ?, "
                + "?, ?, ?, "
                + "?, ?, "
                + "?, ?, "
                + "?, 0, "
                + "'PR', "
                + "?, ?, ?, "
                + "NOW()"
                + ")";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int i = 1;
            ps.setDate(i++, docDate);                  // doc_date          date
            ps.setString(i++, docTime);                // doc_time          varchar(5)
            ps.setString(i++, docNo);                  // doc_no            varchar(25)
            ps.setShort(i++, (short) inquiryType);     // inquiry_type      smallint
            ps.setString(i++, custCode);               // cust_code         varchar(25)
            ps.setString(i++, branchCode);             // branch_code       varchar(25)
            ps.setBigDecimal(i++, vatRate);            // vat_rate          numeric
            ps.setBigDecimal(i++, totalValue);         // total_value       numeric
            ps.setBigDecimal(i++, totalBeforeVat);     // total_before_vat  numeric
            ps.setBigDecimal(i++, totalVatValue);      // total_vat_value   numeric
            ps.setBigDecimal(i++, totalAfterVat);      // total_after_vat   numeric
            ps.setBigDecimal(i++, totalExceptVat);     // total_except_vat  numeric
            ps.setBigDecimal(i++, totalAmount);        // total_amount      numeric
            ps.setBigDecimal(i++, totalAmount);        // balance_amount    numeric (= total_amount)
            ps.setString(i++, creatorCode);            // user_request      varchar(25)
            ps.setString(i++, remark);                 // remark            varchar(255)
            ps.setString(i++, creatorCode);            // creator_code      varchar(255)
            ps.setString(i++, empCode);                // sale_code         varchar(25)
            ps.executeUpdate();
        }
    }

    private void insertDetails(Connection conn, JSONArray items,
            String docNo, java.sql.Date docDate, String docTime,
            String custCode) throws SQLException {

        String sql = "INSERT INTO ic_trans_detail ("
                + "trans_type, trans_flag, " // smallint, smallint
                + "doc_date, doc_time, doc_no, " // date, varchar(5), varchar(25)
                + "cust_code, " // varchar(25)
                + "item_code, item_name, unit_code, " // varchar(25), varchar(255), varchar(25)
                + "qty, price, sum_amount, " // numeric x3
                + "line_number, " // integer
                + "stand_value, divide_value, ratio, " // numeric x3
                + "calc_flag, vat_type, " // integer, integer
                + "doc_date_calc, doc_time_calc, " // date, varchar(5)
                + "create_datetime" // timestamp
                + ") VALUES ("
                + "1, 2, "
                + "?, ?, ?, "
                + "?, "
                + "?, ?, ?, "
                + "?, ?, ?, "
                + "?, "
                + "?, ?, ?, "
                + "1, 1, "
                + "?, ?, "
                + "NOW()"
                + ")";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int n = 0; n < items.length(); n++) {
                JSONObject item = items.getJSONObject(n);

                int i = 1;
                ps.setDate(i++, docDate);                                             // doc_date        date
                ps.setString(i++, docTime);                                           // doc_time        varchar(5)
                ps.setString(i++, docNo);                                             // doc_no          varchar(25)
                ps.setString(i++, custCode);                                          // cust_code       varchar(25)
                ps.setString(i++, item.getString("item_code"));                       // item_code       varchar(25)
                ps.setString(i++, item.getString("item_name"));                       // item_name       varchar(255)
                ps.setString(i++, item.getString("unit_code"));                       // unit_code       varchar(25)
                ps.setBigDecimal(i++, new BigDecimal(item.getString("qty")));          // qty             numeric
                ps.setBigDecimal(i++, new BigDecimal(item.getString("price")));        // price           numeric
                ps.setBigDecimal(i++, new BigDecimal(item.getString("sum_amount")));   // sum_amount      numeric
                ps.setInt(i++, n);                                                    // line_number     integer
                ps.setBigDecimal(i++, new BigDecimal(item.getString("stand_value")));  // stand_value     numeric
                ps.setBigDecimal(i++, new BigDecimal(item.getString("divide_value"))); // divide_value    numeric
                ps.setBigDecimal(i++, new BigDecimal(item.getString("ratio")));        // ratio           numeric
                ps.setDate(i++, docDate);                                             // doc_date_calc   date
                ps.setString(i++, docTime);                                           // doc_time_calc   varchar(5)

                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void clearCartTemp(Connection conn, String custCode) throws SQLException {
        String sql = "DELETE FROM ps_cart_order_temp WHERE cust_code = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, custCode);
            ps.executeUpdate();
        }
    }

// ============================================================
// Utility
// ============================================================
    private String getStr(JSONObject obj, String key, String defaultValue) {
        return obj.has(key) ? obj.getString(key) : defaultValue;
    }

    private BigDecimal getBigDecimal(JSONObject obj, String key, BigDecimal defaultValue) {
        if (!obj.has(key)) {
            return defaultValue;
        }
        String val = obj.getString(key);
        return (val == null || val.isEmpty()) ? defaultValue : new BigDecimal(val);
    }

    private Response badRequest(String message) {
        return Response.status(400)
                .entity("{\"error\": \"" + message + "\"}")
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

    @POST
    @Path("/pay")
    public Response updateTrans(String data) throws Exception {
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {

            String doc_date = "";
            String send_date = "";
            String doc_no_rc = "";
            String cust_code = "";
            String branch_code = "";
            String send_type = "";
            String vat_rate = "0";

            String doc_time = "";
            String address = "";
            String address_name = "";
            String creator_code = "";
            String telephone = "";
            String discount = "";
            String discount_amount = "";
            String total_discount = "0";
            String remark = "";
            String inquiry_type = "";
            String vat_type = "";
            String logistic_area = "";
            String latitude = "";
            String emp_code = "";

            String cash_amount = "";
            String tranfer_amount = "";
            String credit_amount = "";
            String card_amount = "";
            String partial_pay = "0";
            String wallet_amount = "0";
            String total_amount = "0";
            String trans_number = "0";
            String pay_amount = "0";
            String no_approved = "0";
            JSONArray items = new JSONArray();
            JSONArray payments = new JSONArray();
            JSONArray docList = new JSONArray();
            JSONArray wh_list = new JSONArray();
            if (data != null) {
                JSONObject objJSData = new JSONObject(data);

                doc_no_rc = objJSData.has("doc_no") ? objJSData.getString("doc_no") : "";
                doc_time = objJSData.has("doc_time") ? objJSData.getString("doc_time") : "";
                cust_code = objJSData.has("cust_code") ? objJSData.getString("cust_code") : "";
                doc_date = objJSData.has("doc_date") ? objJSData.getString("doc_date") : "";
                wallet_amount = objJSData.has("wallet_amount") ? objJSData.get("wallet_amount").toString() : "0";
                total_amount = objJSData.has("total_amount") ? objJSData.get("total_amount").toString() : "0";
                trans_number = objJSData.has("trans_number") ? objJSData.get("trans_number").toString() : "0";
                no_approved = objJSData.has("no_approved") ? objJSData.get("no_approved").toString() : "0";
                branch_code = objJSData.has("branch_code") ? objJSData.get("branch_code").toString() : "0000";
                vat_rate = "7";

                address = "";
                address_name = "";
                emp_code = objJSData.has("emp_code") ? objJSData.getString("emp_code") : "";
                creator_code = objJSData.has("emp_code") ? objJSData.getString("emp_code") : "";
                telephone = "";
                total_discount = "0";
                remark = objJSData.has("remark") ? objJSData.getString("remark") : "";
                inquiry_type = "1";
                vat_type = "1";
                logistic_area = "";
                latitude = "0";

                if (objJSData.has("doc_detail")) {
                    docList = objJSData.getJSONArray("doc_detail");
                }

            }

            UUID uuid = UUID.randomUUID();
            String strGUID = uuid.toString();
            String strProvider = "DEMO";
            String strDatabaseName = "demo1";

            StringBuilder __result = new StringBuilder();
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String credit_day = "0";
            String credit_date = doc_date;
            String expier_date = doc_date;
            String expire_day = "0";
            if (docList.length() > 0) {

                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmm");
                String dateTime = LocalDateTime.now().format(formatter);
                Random random = new Random();
                int randomNumber = 1000 + random.nextInt(9000);

                String doc_format = "WRC";
                // doc_no_rc = doc_format + dateTime + "-" + randomNumber;

                String __strQUERYCBz = "delete from ap_ar_trans_detail where doc_no = '" + doc_no_rc + "' ;delete from ap_ar_trans where doc_no = '" + doc_no_rc + "' ;delete from cb_trans where doc_no = '" + doc_no_rc + "';delete from cb_trans_detail where doc_no = '" + doc_no_rc + "' ; ";
                System.out.println("" + __strQUERYCBz);
                Statement __stmtzcbz;
                __stmtzcbz = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                __stmtzcbz.executeUpdate(__strQUERYCBz);
                __stmtzcbz.close();

                String __strQUERYArDetail = "";
                for (int i = 0; i < docList.length(); i++) {
                    JSONObject objJSDataItem = docList.getJSONObject(i);

                    String _deptamount = "";
                    String _balance_ref = "";
                    System.out.println("objJSDataItem.get(\"trans_flag\").toString() " + objJSDataItem.get("trans_flag").toString());
                    String __strQUERYMainx = "update ic_trans set used_status_2='1' where doc_no = '" + objJSDataItem.getString("doc_no") + "' and trans_flag = 44 ;";
                    System.out.println("__strQUERYMain" + __strQUERYMainx);
                    Statement __stmtMainx;
                    __stmtMainx = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    __stmtMainx.executeUpdate(__strQUERYMainx);
                    __stmtMainx.close();

                    _deptamount = objJSDataItem.get("total_amount").toString();
                    _balance_ref = objJSDataItem.get("total_amount").toString();

                    __strQUERYArDetail += "insert into ap_ar_trans_detail (trans_type,trans_flag,doc_date,doc_no,billing_no,billing_date,due_date,sum_debt_amount,sum_pay_money,balance_ref,calc_flag,line_number,bill_type) values "
                            + "(2,239,'" + doc_date + "','" + doc_no_rc + "','" + objJSDataItem.getString("doc_no") + "','" + objJSDataItem.getString("doc_date") + "','" + doc_date + "','" + _deptamount + "','" + _deptamount + "','" + _balance_ref + "',0,'" + i + "','44');";

                }
                System.out.println("" + __strQUERYArDetail);
                if (!__strQUERYArDetail.equals("")) {
                    Statement __stmtAr;
                    __stmtAr = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    __stmtAr.executeUpdate(__strQUERYArDetail);
                    __stmtAr.close();
                }

                String __strApAr = "insert into ap_ar_trans (trans_type,trans_flag,doc_date,doc_time,doc_no,doc_format_code,cust_code,branch_code,total_net_value,creator_code) values "
                        + "(2,239,'" + doc_date + "','" + doc_time + "','" + doc_no_rc + "','EE','" + cust_code + "','" + branch_code + "','" + total_amount + "','" + creator_code + "');";

                Statement __stmtApAr;
                System.out.println("" + __strApAr);
                __stmtApAr = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                __stmtApAr.executeUpdate(__strApAr);
                __stmtApAr.close();

                String __strCbBalance = "insert into cb_trans (trans_type,trans_flag,doc_no,doc_date,doc_time,ap_ar_code,pay_type,doc_format_code,total_amount,total_net_amount,total_amount_pay,wallet_amount) values "
                        + "(2,239,'" + doc_no_rc + "','" + doc_date + "','" + doc_time + "','" + cust_code + "',1,'EE','" + total_amount + "','" + wallet_amount + "','" + wallet_amount + "','" + wallet_amount + "');";
                Statement __stmtCbBalance;
                System.out.println("" + __strCbBalance);
                __stmtCbBalance = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                __stmtCbBalance.executeUpdate(__strCbBalance);
                __stmtCbBalance.close();

                String __strQUERYPayAr = "";

                __strQUERYPayAr = "insert into cb_trans_detail (trans_type,trans_flag,doc_no,doc_date,doc_time,trans_number,credit_card_type,amount,sum_amount,doc_type,ap_ar_code,trans_number_type,ap_ar_type,ref1,no_approved) values "
                        + "(2,239,'" + doc_no_rc + "','" + doc_date + "','" + doc_time + "','" + trans_number + "','NONE','" + wallet_amount + "','" + wallet_amount + "','21','" + cust_code + "',1,1,'" + doc_no_rc + "','" + no_approved + "');";

                System.out.println("" + __strQUERYPayAr);
                Statement __stmtPayAr;
                __stmtPayAr = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                __stmtPayAr.executeUpdate(__strQUERYPayAr);
                __stmtPayAr.close();

                __objResponse.put("msg", "success");
                __objResponse.put("success", true);

                __conn.close();
            }
        } catch (JSONException ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(__objResponse.toString(), MediaType.APPLICATION_JSON).build();
    }

    @POST
    @Path("/cancelOrder")
    public Response cancelOrder(String data) throws Exception {
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {

            String doc_date = "";
            String doc_ref = "";
            String send_date = "";
            String doc_no = "";
            String cust_code = "";
            String branch_code = "";
            String send_type = "";
            String vat_rate = "0";
            String total_value = "0";
            String total_vat_value = "0";
            String total_after_vat = "0";
            String total_amount = "0";
            String total_before_vat = "0";
            String doc_time = "";
            String address = "";
            String address_name = "";
            String creator_code = "";
            String telephone = "";
            String discount = "";
            String discount_amount = "";
            String total_discount = "0";
            String remark = "";
            String inquiry_type = "";
            String vat_type = "";
            String logistic_area = "";
            String latitude = "";
            String emp_code = "";
            JSONArray items = new JSONArray();
            JSONArray wh_list = new JSONArray();
            if (data != null) {
                JSONObject objJSData = new JSONObject(data);
                doc_date = objJSData.has("doc_date") ? objJSData.getString("doc_date") : "";
                send_date = objJSData.has("doc_date") ? objJSData.getString("doc_date") : "";
                send_type = objJSData.has("send_type") ? objJSData.getString("send_type") : "0";
                doc_time = objJSData.has("doc_time") ? objJSData.getString("doc_time") : "";
                doc_ref = objJSData.has("doc_ref") ? objJSData.getString("doc_ref") : "";
                doc_no = objJSData.has("doc_no") ? objJSData.getString("doc_no") : "";
                cust_code = objJSData.has("cust_code") ? objJSData.getString("cust_code") : "";
                branch_code = objJSData.has("branch_code") ? objJSData.getString("branch_code") : "0000";
                vat_rate = "7";
                total_value = objJSData.has("total_value") ? objJSData.getString("total_value") : "0";
                total_after_vat = objJSData.has("total_amount") ? objJSData.getString("total_amount") : "0";
                total_amount = objJSData.has("total_amount") ? objJSData.getString("total_amount") : "0";
                total_before_vat = (Float.parseFloat(total_after_vat) * 100 / (100 + Float.parseFloat(vat_rate))) + "";
                total_vat_value = (Float.parseFloat(total_after_vat) - Float.parseFloat(total_before_vat)) + "";
//                address = objJSData.has("address") ? objJSData.getString("address") : "";
                emp_code = objJSData.has("emp_code") ? objJSData.getString("emp_code") : "";
                creator_code = objJSData.has("cust_code") ? objJSData.getString("cust_code") : "";
                telephone = objJSData.has("telephone") ? objJSData.getString("telephone") : "";
                total_discount = "0";
                remark = objJSData.has("remark") ? objJSData.getString("remark") : "";
                inquiry_type = "0";
                vat_type = "1";
                logistic_area = "";
                latitude = "";
                if (objJSData.has("items")) {
                    items = objJSData.getJSONArray("items");
                }
            }

            UUID uuid = UUID.randomUUID();
            String strGUID = uuid.toString();
            String strProvider = "DEMO";
            String strDatabaseName = "demo1";

            StringBuilder __result = new StringBuilder();
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String credit_day = "0";
            String credit_date = doc_date;
            String expier_date = doc_date;
            String expire_day = "0";
            if (items.length() > 0) {

                String __strQUERY0 = "select doc_no from ic_trans where doc_no = '" + doc_no + "' and doc_format_code = 'SOC'";
                System.out.println("__strQUERY0" + __strQUERY0);
                Statement __stmt0;
                ResultSet __rs0;
                __stmt0 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                __rs0 = __stmt0.executeQuery(__strQUERY0);
                __rs0.next();
                int row0 = __rs0.getRow();
                System.out.println("row0 " + row0);
                if (row0 == 0) {

                    String __strQUERYMain = "insert into ic_trans "
                            + "(inquiry_type,vat_type,trans_type,trans_flag,doc_date,doc_no,doc_ref,tax_doc_no,tax_doc_date,cust_code,branch_code,send_date,vat_rate,total_value,"
                            + "total_vat_value,total_after_vat,total_amount,\n"
                            + "total_before_vat,doc_time,doc_format_code,creator_code,sale_code,total_discount,remark,send_type) "
                            + "values "
                            + "('" + inquiry_type + "','" + vat_type + "',2,31,'" + doc_date + "','" + doc_no + "','" + doc_ref + "','" + doc_no + "','" + doc_date + "','" + cust_code + "','" + branch_code + "','" + send_date + "','" + vat_rate + "','" + total_value + "','" + total_vat_value + "','" + total_after_vat + "','" + total_amount + "','" + total_before_vat + "','" + doc_time + "','SOC','" + creator_code + "','" + emp_code + "','" + total_discount + "','" + remark + "','" + send_type + "')";
                    System.out.println("__strQUERYMain" + __strQUERYMain);
                    Statement __stmtMain;
                    __stmtMain = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    __stmtMain.executeUpdate(__strQUERYMain);
                    __stmtMain.close();

                    String __strQUERYz = "delete from ic_trans_detail where doc_no = '" + doc_no + "' ";
                    System.out.println("" + __strQUERYz);
                    Statement __stmtz;
                    __stmtz = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    __stmtz.executeUpdate(__strQUERYz);
                    __stmtz.close();

                    for (int i = 0; i < items.length(); i++) {
                        JSONObject objJSDataItem = items.getJSONObject(i);

                        String __strQUERY1 = "insert into ic_trans_detail (inquiry_type,vat_type,trans_type,trans_flag,doc_date,doc_no,cust_code,item_code,item_name,unit_code,qty,price,sum_amount,line_number,remark,wh_code,shelf_code,stand_value,divide_value,ratio,doc_time,doc_date_calc,discount,discount_amount) values "
                                + "('" + inquiry_type + "','" + vat_type + "',2,31,'" + doc_date + "','" + doc_no + "','" + cust_code + "','" + objJSDataItem.getString("item_code") + "','" + objJSDataItem.getString("item_name") + "','" + objJSDataItem.getString("unit_code") + "','" + objJSDataItem.getString("qty") + "','" + objJSDataItem.getString("price") + "','" + objJSDataItem.getString("sum_amount") + "',"
                                + "'" + i + "','','" + objJSDataItem.getString("wh_code") + "','" + objJSDataItem.getString("shelf_code") + "','" + objJSDataItem.getString("stand_value") + "','" + objJSDataItem.getString("divide_value") + "','" + objJSDataItem.getString("ratio") + "','" + doc_time + "','" + doc_date + "','','0');";
                        System.out.println("" + __strQUERY1);
                        Statement __stmt1;
                        __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                        __stmt1.executeUpdate(__strQUERY1);
                        __stmt1.close();

                    }

                    String __strShipment = "update ic_trans set last_status = 1 where doc_no = '" + doc_ref + "'";
                    Statement __stmtShipment;
                    __stmtShipment = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    __stmtShipment.executeUpdate(__strShipment);
                    __stmtShipment.close();

                    __objResponse.put("msg", "success");
                    __objResponse.put("success", true);

                    __rs0.close();
                    __stmt0.close();
                } else {
                    __objResponse.put("msg", "Duplicate Doc No.");
                    __objResponse.put("success", false);
                }
                __conn.close();
            }
        } catch (JSONException ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(__objResponse.toString(), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/loginemp")
    public Response login(
            @QueryParam("user_code") String strUserCode,
            @QueryParam("password") String strPassword) {
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect("demo1", _global.FILE_CONFIG("DEMO"));
            String __strQUERY1 = "SELECT code as user_code, name_1 as user_name FROM erp_user WHERE upper(code)=upper('" + strUserCode + "') AND password='" + strPassword + "' ORDER BY code";

            Statement __stmt1;
            ResultSet __rs1;
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);
            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("user_code", __rs1.getString("user_code"));
                obj.put("user_name", __rs1.getString("user_name"));

                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/logincus")
    public Response logincus(
            @QueryParam("user_code") String strUserCode,
            @QueryParam("password") String strPassword) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String __strQUERY1 = "SELECT code as user_code,name_1 as user_name,address,telephone,coalesce((select tax_id from ar_customer_detail where ar_code = code),'') as tax_id FROM ar_customer WHERE upper(code)=upper('" + strUserCode + "') AND country='" + strPassword + "' ORDER BY code";

            Statement __stmt1;
            ResultSet __rs1;
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("user_code", __rs1.getString("user_code"));
                obj.put("user_name", __rs1.getString("user_name"));
                obj.put("address", __rs1.getString("address"));
                obj.put("telephone", __rs1.getString("telephone"));
                obj.put("tax_id", __rs1.getString("tax_id"));

                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/setfav")
    public Response setFav(
            @QueryParam("status") String status,
            @QueryParam("cust_code") String strCust,
            @QueryParam("item_code") String strItem) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));

            Statement __stmtdelete = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __stmtdelete.executeUpdate("delete from ar_item_by_customer where ar_code = '" + strCust + "' and ic_code = '" + strItem + "' ;");
            __stmtdelete.close();

            String __strQUERY1 = "insert into ar_item_by_customer (status,ic_code,ar_code) values('" + status + "','" + strItem + "','" + strCust + "')";

            Statement __stmtMain;
            __stmtMain = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __stmtMain.executeUpdate(__strQUERY1);
            __stmtMain.close();

            __objResponse.put("success", true);
            __conn.close();
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getAdvancePayment")
    public Response getAdvancePayment(
            @QueryParam("cust_code") String strCust
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String _where = "";

            String __strQUERY1 = "select cust_code as cust_code,cust_name as cust_name,\n"
                    + "case when _def_last_status = 1 then 0 else deposit_buy2 end as deposit_buy2,\n"
                    + "case when _def_last_status = 1 then 0 else sum_used end as sum_used,\n"
                    + "case when _def_last_status = 1 then 0 else total_amount-(deposit_buy2+sum_used) end as balance_amount,\n"
                    + "doc_date as doc_date,doc_no as doc_no,doc_ref_date as doc_ref_date,\n"
                    + "doc_ref as doc_ref,total_amount as total_amount,vat_type as vat_type,\n"
                    + "vat_rate as vat_rate,last_status as last_status \n"
                    + "from (select cust_code,\n"
                    + "(select name_1 from ar_customer where ic_trans.cust_code=ar_customer.code) as cust_name,\n"
                    + "coalesce((select sum(total_amount) from ic_trans as x1 where x1.last_status=0 and x1.doc_ref=ic_trans.doc_no and x1.trans_flag in (112,42)),0) as deposit_buy2,\n"
                    + "coalesce((select sum(amount) from cb_trans_detail as x2 where x2.last_status=0 and x2.trans_number=ic_trans.doc_no and x2.trans_flag not in (40,110)),0) as sum_used,\n"
                    + "doc_date,doc_no,doc_time,cashier_code,doc_ref_date,doc_ref,total_amount,total_before_vat,vat_type,\n"
                    + "vat_rate,last_status as _def_last_status,\n"
                    + "cast(last_status as varchar)||','||cast(used_status as varchar)||','||cast(doc_success as varchar)||','||cast(not_approve_1 as varchar)||','||cast(on_hold as varchar)||','||cast(approve_status as varchar)||','||cast(expire_status  as varchar)||','||cast(used_status_2  as varchar) as last_status \n"
                    + "from ic_trans  \n"
                    + "where trans_flag in (40,9040)  \n"
                    + "and is_doc_copy <> 1 \n"
                    + "and cust_code = '" + strCust + "') as temp1 \n"
                    + "order by doc_date desc,doc_no limit 20";

            Statement __stmt1;
            ResultSet __rs1;

            System.out.println(__strQUERY1);
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("doc_no", __rs1.getString("doc_no"));
                obj.put("doc_date", __rs1.getString("doc_date"));
                obj.put("total_amount", __rs1.getString("total_amount"));
                obj.put("used", __rs1.getString("sum_used"));
                obj.put("balance_amount", __rs1.getString("balance_amount"));

                __jsonArr.put(obj);

            }
            __rs1.close();
            __conn.close();
            __objResponse.put("data", __jsonArr);
            __objResponse.put("success", true);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getOrderHistory")
    public Response getOrderHistory(
            @QueryParam("cust_code") String strCust,
            @QueryParam("status") String strStatus
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String _where = "";
            if (!strStatus.equals("")) {
                if (!strStatus.equals("partial")) {
                    _where += " and status = '" + strStatus + "' ";
                }
            }

//            String __strQUERY1 = "select * from (select COALESCE(ic_inv.remark_5,'') as remark_5,COALESCE(ap_inv.doc_no,'') as inv_doc_no,COALESCE(ap_ar_cb.wallet_amount,0) as wallet_amount,COALESCE(ic_qt.remark,'') as remark_qt,COALESCE(ic_soc.remark,'') as remark_cancel,COALESCE(ap_inv.remark,'') as remark_inv,ic_qt.doc_no,ic_qt.doc_date,ic_qt.doc_time,ic_qt.cust_code,ic_qt.send_type,ic_qt.sale_code as emp_code,coalesce((select name_1 from erp_user where upper(code) = upper(ic_qt.sale_code) limit 1),'') as emp_name,ic_qt.total_amount,ic_qt.total_except_vat,ic_qt.total_after_vat,ic_qt.total_vat_value,\n"
//                    + "COALESCE((select \n"
//                    + "                     balance_amount \n"
//                    + "                    from (select cust_code, doc_date , credit_date as due_date , doc_no , \n"
//                    + "                    trans_flag as doc_type , used_status , doc_ref as ref_doc_no , doc_ref_date as ref_doc_date , coalesce(total_amount,0) as amount , \n"
//                    + "                    coalesce(total_amount,0)-(select coalesce(sum(coalesce(sum_pay_money,0)),0) from ap_ar_trans_detail \n"
//                    + "                    where coalesce(last_status, 0)=0 and trans_flag in (239) and ic_trans.doc_no=ap_ar_trans_detail.billing_no \n"
//                    + "                    and ic_trans.doc_date=ap_ar_trans_detail.billing_date) as balance_amount,branch_code  \n"
//                    + "                    from ic_trans  where  coalesce(last_status, 0)=0  and trans_flag=44 and (inquiry_type=0  or inquiry_type=2) and cust_code= ic_qt.cust_code \n"
//                    + "                    \n"
//                    + "                    union all select cust_code , doc_date , credit_date as due_date , \n"
//                    + "                    doc_no , trans_flag as doc_type , used_status , '' as ref_doc_no , \n"
//                    + "                    null as ref_doc_date , coalesce(total_amount,0) as amount , coalesce(total_amount,0)-(select coalesce(sum(coalesce(sum_pay_money,0)),0) \n"
//                    + "                    from ap_ar_trans_detail where coalesce(last_status, 0)=0 and trans_flag in (239) and ic_trans.doc_no=ap_ar_trans_detail.billing_no \n"
//                    + "                    and ic_trans.doc_date=ap_ar_trans_detail.billing_date ) as balance_amount,branch_code  from ic_trans  where  coalesce(last_status, 0)=0   \n"
//                    + "                    and (trans_flag=46 or trans_flag=93 or trans_flag=99 or trans_flag=95 or trans_flag=101)  and cust_code=ic_qt.cust_code \n"
//                    + "                    \n"
//                    + "                    union all select cust_code , doc_date , credit_date as due_date , doc_no , trans_flag as doc_type , used_status , '' as ref_doc_no , \n"
//                    + "                    null as ref_doc_date , -1*coalesce(total_amount,0) as amount , -1*(coalesce(total_amount,0)+(select coalesce(sum(coalesce(sum_pay_money,0)),0) \n"
//                    + "                    from ap_ar_trans_detail where coalesce(last_status, 0)=0 and trans_flag in (239) and ic_trans.doc_no=ap_ar_trans_detail.billing_no \n"
//                    + "                    and ic_trans.doc_date=ap_ar_trans_detail.billing_date)) as balance_amount,branch_code from ic_trans  where  coalesce(last_status, 0)=0 \n"
//                    + "                    and ((trans_flag=48 and inquiry_type in (0,2,4) ) or trans_flag=97 or trans_flag=103)  and cust_code=ic_qt.cust_code ) as xx \n"
//                    + "                    where balance_amount  <> 0 and doc_no = ap_ar.billing_no order by cust_code, doc_date, doc_no limit 1 ),0) as balance,\n"
//                    + "                    case when ap_ar.doc_no is not null THEN 'success' when ap_ar.doc_no is null and ap_inv.doc_no is not null THEN 'payment' when  ap_ar.doc_no is null and ap_inv.doc_no is null and ap_so.doc_no is not null THEN 'packing' when ic_soc.doc_no is not null THEN 'cancel' else 'pending' end as status\n"
//                    + "from ic_trans ic_qt left join \n"
//                    + "ic_trans ic_soc on ic_soc.doc_ref = ic_qt.doc_no and ic_soc.trans_flag = 31 left join \n"
//                    + "ap_ar_trans_detail ap_so on ap_so.billing_no = ic_qt.doc_no and ap_so.trans_flag = 36 left join \n"
//                    + "ap_ar_trans_detail ap_inv on ap_inv.billing_no = ap_so.doc_no and ap_inv.trans_flag = 44  left join \n"
//                    + "ic_trans ic_inv on ic_inv.doc_no = ap_inv.doc_no and ic_inv.trans_flag = 44  left join \n"
//                    + "ap_ar_trans_detail ap_ar on ap_ar.billing_no = ap_inv.doc_no and ap_ar.trans_flag = 239  left join \n"
//                    + "cb_trans ap_ar_cb on ap_ar_cb.doc_no = ap_ar.doc_no and ap_ar_cb.trans_flag = 239  "
//                    + " where ic_qt.trans_flag = 30  and ic_qt.cust_code = '" + strCust + "' and ic_qt.doc_no like '%MQT%' order by doc_date desc  ) as temp where 1=1 " + _where + " order by doc_date desc  limit 40 ";
            String __strQUERY1 = "WITH SUM_CN AS (\n"
                    + "select b.ref_doc_no as ref_doc_no,\n"
                    + "sum(a.total_amount) as cn_total_amount,\n"
                    + "sum(a.total_except_vat) as cn_total_except_vat,\n"
                    + "sum(a.total_after_vat) as cn_total_after_vat,\n"
                    + "sum(a.total_vat_value) as cn_total_vat_value\n"
                    + "from ic_trans a\n"
                    + "left join ic_trans_detail b on a.doc_no=b.doc_no\n"
                    + "\n"
                    + "group by b.ref_doc_no\n"
                    + ")\n"
                    + "\n"
                    + "SELECT DISTINCT *\n"
                    + "FROM (\n"
                    + "    SELECT\n"
                    + "        COALESCE(ic_inv.remark_5, '') AS remark_5,\n"
                    + "        COALESCE(ap_inv.doc_no, '') AS inv_doc_no,\n"
                    + "        COALESCE(ap_inv.doc_date::text, '') AS inv_doc_date,\n"
                    + "        COALESCE(ap_ar_cb.wallet_amount, 0) AS wallet_amount,\n"
                    + "        COALESCE(ic_qt.wh_from, '') AS wh_code,\n"
                    + "        COALESCE((select name_1 from ic_warehouse where code = ic_qt.wh_from ), '') AS wh_name,\n"
                    + "        COALESCE(ic_qt.inquiry_type, 0) AS inquiry_type,\n"
                    + "        COALESCE(ic_qt.remark, '') AS remark_qt,\n"
                    + "        COALESCE(ic_soc.remark, '') AS remark_cancel,\n"
                    + "        COALESCE(ap_inv.remark, '') AS remark_inv,\n"
                    + "\n"
                    + "        ic_qt.doc_no,\n"
                    + "        ic_qt.doc_date,\n"
                    + "        ic_qt.doc_time,\n"
                    + "        ic_qt.cust_code,\n"
                    + "        ic_qt.send_type,\n"
                    + "        ic_qt.sale_code AS emp_code,\n"
                    + "\n"
                    + "        COALESCE((\n"
                    + "            SELECT name_1\n"
                    + "            FROM erp_user\n"
                    + "            WHERE UPPER(code) = UPPER(ic_qt.sale_code)\n"
                    + "            LIMIT 1\n"
                    + "        ), '') AS emp_name,\n"
                    + "\n"
                    + "        ic_qt.total_amount-coalesce((select cn_total_amount from SUM_CN where ic_inv.doc_no=SUM_CN.ref_doc_no),0) as total_amount,"
                    + "        coalesce((select cn_total_amount from SUM_CN where ic_inv.doc_no=SUM_CN.ref_doc_no),0) as cn_total_amount,"
                    + "        ic_qt.total_except_vat-coalesce((select cn_total_except_vat from SUM_CN where ic_inv.doc_no=SUM_CN.ref_doc_no),0) as total_except_vat,\n"
                    + "        ic_qt.total_after_vat-coalesce((select cn_total_after_vat from SUM_CN where ic_inv.doc_no=SUM_CN.ref_doc_no),0) as total_after_vat,\n"
                    + "        ic_qt.total_vat_value-coalesce((select cn_total_vat_value from SUM_CN where ic_inv.doc_no=SUM_CN.ref_doc_no),0) as total_vat_value,\n"
                    + "\n"
                    + "        COALESCE((\n"
                    + "            SELECT balance_amount\n"
                    + "            FROM (\n"
                    + "                SELECT\n"
                    + "                    cust_code, doc_date, credit_date AS due_date, doc_no,\n"
                    + "                    trans_flag AS doc_type, used_status, doc_ref AS ref_doc_no,\n"
                    + "                    doc_ref_date AS ref_doc_date,\n"
                    + "                    COALESCE(total_amount, 0) AS amount,\n"
                    + "                    COALESCE(total_amount, 0) - (\n"
                    + "                        SELECT COALESCE(SUM(COALESCE(sum_pay_money, 0)), 0)\n"
                    + "                        FROM ap_ar_trans_detail\n"
                    + "                        WHERE COALESCE(last_status, 0) = 0\n"
                    + "                            AND trans_flag IN (239)\n"
                    + "                            AND ic_trans.doc_no = ap_ar_trans_detail.billing_no\n"
                    + "                            AND ic_trans.doc_date = ap_ar_trans_detail.billing_date\n"
                    + "                    ) AS balance_amount,\n"
                    + "                    branch_code\n"
                    + "                FROM ic_trans\n"
                    + "                WHERE COALESCE(last_status, 0) = 0\n"
                    + "                    AND trans_flag = 44\n"
                    + "                    AND inquiry_type IN (0, 2)\n"
                    + "                    AND cust_code = ic_qt.cust_code\n"
                    + "\n"
                    + "                UNION ALL\n"
                    + "\n"
                    + "                SELECT\n"
                    + "                    cust_code, doc_date, credit_date AS due_date, doc_no,\n"
                    + "                    trans_flag AS doc_type, used_status, '' AS ref_doc_no,\n"
                    + "                    NULL AS ref_doc_date,\n"
                    + "                    COALESCE(total_amount, 0) AS amount,\n"
                    + "                    COALESCE(total_amount, 0) - (\n"
                    + "                        SELECT COALESCE(SUM(COALESCE(sum_pay_money, 0)), 0)\n"
                    + "                        FROM ap_ar_trans_detail\n"
                    + "                        WHERE COALESCE(last_status, 0) = 0\n"
                    + "                            AND trans_flag IN (239)\n"
                    + "                            AND ic_trans.doc_no = ap_ar_trans_detail.billing_no\n"
                    + "                            AND ic_trans.doc_date = ap_ar_trans_detail.billing_date\n"
                    + "                    ) AS balance_amount,\n"
                    + "                    branch_code\n"
                    + "                FROM ic_trans\n"
                    + "                WHERE COALESCE(last_status, 0) = 0\n"
                    + "                    AND trans_flag IN (46, 93, 95, 99, 101)\n"
                    + "                    AND cust_code = ic_qt.cust_code\n"
                    + "\n"
                    + "                UNION ALL\n"
                    + "\n"
                    + "                SELECT\n"
                    + "                    cust_code, doc_date, credit_date AS due_date, doc_no,\n"
                    + "                    trans_flag AS doc_type, used_status, '' AS ref_doc_no,\n"
                    + "                    NULL AS ref_doc_date,\n"
                    + "                    -1 * COALESCE(total_amount, 0) AS amount,\n"
                    + "                    -1 * (COALESCE(total_amount, 0) + (\n"
                    + "                            SELECT COALESCE(SUM(COALESCE(sum_pay_money, 0)), 0)\n"
                    + "                            FROM ap_ar_trans_detail\n"
                    + "                            WHERE COALESCE(last_status, 0) = 0\n"
                    + "                                AND trans_flag IN (239)\n"
                    + "                                AND ic_trans.doc_no = ap_ar_trans_detail.billing_no\n"
                    + "                                AND ic_trans.doc_date = ap_ar_trans_detail.billing_date\n"
                    + "                        )\n"
                    + "                    ) AS balance_amount,\n"
                    + "                    branch_code\n"
                    + "                FROM ic_trans\n"
                    + "                WHERE COALESCE(last_status, 0) = 0\n"
                    + "                    AND (\n"
                    + "                        (trans_flag = 48 AND inquiry_type IN (0, 2, 4))\n"
                    + "                        OR trans_flag IN (97, 103)\n"
                    + "                    )\n"
                    + "                    AND cust_code = ic_qt.cust_code\n"
                    + "            ) AS xx\n"
                    + "            WHERE balance_amount <> 0\n"
                    + "              AND doc_no = ap_ar.billing_no\n"
                    + "            ORDER BY cust_code, doc_date, doc_no\n"
                    + "            LIMIT 1\n"
                    + "        ), 0) AS balance,\n"
                    + "\n"
                    + "        CASE\n"
                    + "            WHEN ic_soc.doc_no IS NOT NULL OR ic_ssc.doc_no IS NOT NULL THEN 'cancel'\n"
                    + "            WHEN ap_ar.doc_no IS NOT NULL or cb_inv.total_amount_pay > 0 THEN 'success'\n"
                    + "            WHEN ap_ar.doc_no IS NULL AND ap_inv.doc_no IS NOT NULL THEN 'payment'\n"
                    + "            WHEN ap_ar.doc_no IS NULL AND ap_inv.doc_no IS NULL AND ap_so.doc_no IS NOT NULL THEN 'packing'\n"
                    + "            ELSE 'pending'\n"
                    + "        END AS status\n"
                    + "\n"
                    + "    FROM ic_trans ic_qt\n"
                    + "    LEFT JOIN ic_trans ic_soc ON ic_soc.doc_ref = ic_qt.doc_no AND ic_soc.trans_flag = 31\n"
                    + "    LEFT JOIN ap_ar_trans_detail ap_so ON ap_so.billing_no = ic_qt.doc_no AND ap_so.trans_flag = 36\n"
                    + "    LEFT JOIN ic_trans ic_ssc ON ic_ssc.doc_ref = ap_so.doc_no AND ic_ssc.trans_flag = 37\n"
                    + "    LEFT JOIN ap_ar_trans_detail ap_inv ON ap_inv.billing_no = ap_so.doc_no AND ap_inv.trans_flag = 44 "
                    + "    LEFT JOIN ic_trans ic_inv ON ic_inv.doc_no = ap_inv.doc_no AND ic_inv.trans_flag = 44\n"
                    + "    LEFT JOIN cb_trans cb_inv ON cb_inv.doc_no = ap_inv.doc_no AND ic_inv.trans_flag = 44 "
                    + "    LEFT JOIN ap_ar_trans_detail ap_ar ON ap_ar.billing_no = ap_inv.doc_no AND ap_ar.trans_flag = 239\n"
                    + "    LEFT JOIN cb_trans ap_ar_cb ON ap_ar_cb.doc_no = ap_ar.doc_no AND ap_ar_cb.trans_flag = 239\n"
                    + " "
                    + "    WHERE\n"
                    + "        ic_qt.trans_flag = 30\n"
                    + "        AND ic_qt.cust_code = '" + strCust + "' \n"
                    + "        AND ic_qt.doc_no LIKE '%MQT%'\n"
                    + "    ORDER BY doc_date DESC\n"
                    + ") AS temp  WHERE 1 = 1 " + _where + " ORDER BY doc_date DESC , doc_time desc \n"
                    + " LIMIT 40;";

            Statement __stmt1;
            ResultSet __rs1;
//            success=สำเร็จ
//            partial=ชำระแล้วบางส่วน
//            payment=กำลังจัดส่ง / รอรับชำระ
//            packing=กำลังจัดสินค้า
//            cancel=ยกเลิก
//            pending=รอตรวจสอบ
            System.out.println(__strQUERY1);
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {
                if (strStatus.equals("partial")) {
                    if (__rs1.getString("status").equals("success") && Float.parseFloat(__rs1.getString("balance")) > 0) {
                        JSONObject obj = new JSONObject();
                        String sale_type = "1";
                        if (__rs1.getString("inquiry_type").equals("2")) {
                            sale_type = "ขายสด";
                        } else {
                            sale_type = "ขายเชื่อ";
                        }
                        obj.put("wh_code", __rs1.getString("wh_code"));
                        obj.put("wh_name", __rs1.getString("wh_name"));
                        obj.put("sale_type", sale_type);

                        obj.put("doc_no", __rs1.getString("doc_no"));
                        obj.put("doc_date", __rs1.getString("doc_date"));
                        obj.put("doc_time", __rs1.getString("doc_time"));
                        obj.put("cust_code", __rs1.getString("cust_code"));
                        obj.put("send_type", __rs1.getString("send_type"));
                        obj.put("total_amount", __rs1.getString("total_amount"));
                        obj.put("emp_code", __rs1.getString("emp_code"));
                        obj.put("emp_name", __rs1.getString("emp_name"));
                        obj.put("balance", __rs1.getString("balance"));
                        obj.put("remark_qt", __rs1.getString("remark_qt"));
                        obj.put("remark_cancel", __rs1.getString("remark_cancel"));
                        obj.put("remark_inv", __rs1.getString("remark_inv"));
                        obj.put("remark_5", __rs1.getString("remark_5"));
                        obj.put("inv_doc_no", __rs1.getString("inv_doc_no"));
                        obj.put("inv_doc_date", __rs1.getString("inv_doc_date"));
                        obj.put("wallet_amount", __rs1.getString("wallet_amount"));
                        obj.put("total_except_vat", __rs1.getString("total_except_vat"));
                        obj.put("total_after_vat", __rs1.getString("total_after_vat"));
                        obj.put("total_vat_value", __rs1.getString("total_vat_value"));
                        obj.put("cn_total_amount", __rs1.getString("cn_total_amount"));

                        if (__rs1.getString("status").equals("success") && Float.parseFloat(__rs1.getString("balance")) > 0) {
                            obj.put("status", "partial");
                        } else {
                            obj.put("status", __rs1.getString("status"));
                        }

                        __jsonArr.put(obj);
                    }
                } else if (strStatus.equals("success")) {
                    if (__rs1.getString("status").equals("success") && Float.parseFloat(__rs1.getString("balance")) == 0) {
                        JSONObject obj = new JSONObject();

                        String sale_type = "1";
                        if (__rs1.getString("inquiry_type").equals("2")) {
                            sale_type = "ขายสด";
                        } else {
                            sale_type = "ขายเชื่อ";
                        }
                        obj.put("wh_code", __rs1.getString("wh_code"));
                        obj.put("wh_name", __rs1.getString("wh_name"));
                        obj.put("sale_type", sale_type);

                        obj.put("doc_no", __rs1.getString("doc_no"));
                        obj.put("doc_date", __rs1.getString("doc_date"));
                        obj.put("doc_time", __rs1.getString("doc_time"));
                        obj.put("cust_code", __rs1.getString("cust_code"));
                        obj.put("send_type", __rs1.getString("send_type"));
                        obj.put("total_amount", __rs1.getString("total_amount"));
                        obj.put("emp_code", __rs1.getString("emp_code"));
                        obj.put("emp_name", __rs1.getString("emp_name"));
                        obj.put("balance", __rs1.getString("balance"));
                        obj.put("remark_qt", __rs1.getString("remark_qt"));
                        obj.put("remark_cancel", __rs1.getString("remark_cancel"));
                        obj.put("remark_inv", __rs1.getString("remark_inv"));
                        obj.put("remark_5", __rs1.getString("remark_5"));
                        obj.put("inv_doc_no", __rs1.getString("inv_doc_no"));
                        obj.put("inv_doc_date", __rs1.getString("inv_doc_date"));
                        obj.put("wallet_amount", __rs1.getString("wallet_amount"));
                        obj.put("total_except_vat", __rs1.getString("total_except_vat"));
                        obj.put("total_after_vat", __rs1.getString("total_after_vat"));
                        obj.put("total_vat_value", __rs1.getString("total_vat_value"));
                        obj.put("cn_total_amount", __rs1.getString("cn_total_amount"));
                        if (__rs1.getString("status").equals("success") && Float.parseFloat(__rs1.getString("balance")) > 0) {
                            obj.put("status", "partial");
                        } else {
                            obj.put("status", __rs1.getString("status"));
                        }

                        __jsonArr.put(obj);
                    }
                } else {

                    JSONObject obj = new JSONObject();
                    String sale_type = "1";
                    if (__rs1.getString("inquiry_type").equals("2")) {
                        sale_type = "ขายสด";
                    } else {
                        sale_type = "ขายเชื่อ";
                    }
                    obj.put("wh_code", __rs1.getString("wh_code"));
                    obj.put("wh_name", __rs1.getString("wh_name"));
                    obj.put("sale_type", sale_type);

                    obj.put("doc_no", __rs1.getString("doc_no"));
                    obj.put("doc_date", __rs1.getString("doc_date"));
                    obj.put("doc_time", __rs1.getString("doc_time"));
                    obj.put("cust_code", __rs1.getString("cust_code"));
                    obj.put("send_type", __rs1.getString("send_type"));
                    obj.put("total_amount", __rs1.getString("total_amount"));
                    obj.put("emp_code", __rs1.getString("emp_code"));
                    obj.put("emp_name", __rs1.getString("emp_name"));
                    obj.put("balance", __rs1.getString("balance"));
                    obj.put("remark_qt", __rs1.getString("remark_qt"));
                    obj.put("remark_cancel", __rs1.getString("remark_cancel"));
                    obj.put("remark_inv", __rs1.getString("remark_inv"));
                    obj.put("remark_5", __rs1.getString("remark_5"));
                    obj.put("inv_doc_no", __rs1.getString("inv_doc_no"));
                    obj.put("inv_doc_date", __rs1.getString("inv_doc_date"));
                    obj.put("wallet_amount", __rs1.getString("wallet_amount"));
                    obj.put("total_except_vat", __rs1.getString("total_except_vat"));
                    obj.put("total_after_vat", __rs1.getString("total_after_vat"));
                    obj.put("total_vat_value", __rs1.getString("total_vat_value"));
                    obj.put("cn_total_amount", __rs1.getString("cn_total_amount"));
                    if (__rs1.getString("status").equals("success") && Float.parseFloat(__rs1.getString("balance")) > 0) {
                        obj.put("status", "partial");
                    } else {
                        obj.put("status", __rs1.getString("status"));
                    }

                    __jsonArr.put(obj);
                }
            }
            __rs1.close();
            __conn.close();
            __objResponse.put("data", __jsonArr);
            __objResponse.put("success", true);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getOrderDetail")
    public Response getOrderDetail(
            @QueryParam("cust_code") String strCust,
            @QueryParam("doc_no") String strDocNo
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));

            String __strQUERY1 = "select * from (select COALESCE(ic_inv.remark_5,'') as remark_5,COALESCE(its.transport_name,'') as address ,COALESCE(its.transport_telephone,'')as telephone ,COALESCE(its.transport_address,'') as address_name,ic_qt.doc_no,ic_qt.doc_date,ic_qt.doc_time,ic_qt.cust_code,ic_qt.send_type,ic_qt.sale_code as emp_code,coalesce((select name_1 from erp_user where upper(code) = upper(ic_qt.sale_code) limit 1),'') as emp_name,ic_qt.total_amount,ic_qt.total_except_vat,ic_qt.total_after_vat,ic_qt.total_vat_value,\n"
                    + "COALESCE((select \n"
                    + "                     balance_amount \n"
                    + "                    from (select cust_code, doc_date , credit_date as due_date , doc_no , \n"
                    + "                    trans_flag as doc_type , used_status , doc_ref as ref_doc_no , doc_ref_date as ref_doc_date , coalesce(total_amount,0) as amount , \n"
                    + "                    coalesce(total_amount,0)-(select coalesce(sum(coalesce(sum_pay_money,0)),0) from ap_ar_trans_detail \n"
                    + "                    where coalesce(last_status, 0)=0 and trans_flag in (239) and ic_trans.doc_no=ap_ar_trans_detail.billing_no \n"
                    + "                    and ic_trans.doc_date=ap_ar_trans_detail.billing_date) as balance_amount,branch_code  \n"
                    + "                    from ic_trans  where  coalesce(last_status, 0)=0  and trans_flag=44 and (inquiry_type=0  or inquiry_type=2) and cust_code= ic_qt.cust_code \n"
                    + "                    \n"
                    + "                    union all select cust_code , doc_date , credit_date as due_date , \n"
                    + "                    doc_no , trans_flag as doc_type , used_status , '' as ref_doc_no , \n"
                    + "                    null as ref_doc_date , coalesce(total_amount,0) as amount , coalesce(total_amount,0)-(select coalesce(sum(coalesce(sum_pay_money,0)),0) \n"
                    + "                    from ap_ar_trans_detail where coalesce(last_status, 0)=0 and trans_flag in (239) and ic_trans.doc_no=ap_ar_trans_detail.billing_no \n"
                    + "                    and ic_trans.doc_date=ap_ar_trans_detail.billing_date ) as balance_amount,branch_code  from ic_trans  where  coalesce(last_status, 0)=0   \n"
                    + "                    and (trans_flag=46 or trans_flag=93 or trans_flag=99 or trans_flag=95 or trans_flag=101)  and cust_code=ic_qt.cust_code \n"
                    + "                    \n"
                    + "                    union all select cust_code , doc_date , credit_date as due_date , doc_no , trans_flag as doc_type , used_status , '' as ref_doc_no , \n"
                    + "                    null as ref_doc_date , -1*coalesce(total_amount,0) as amount , -1*(coalesce(total_amount,0)+(select coalesce(sum(coalesce(sum_pay_money,0)),0) \n"
                    + "                    from ap_ar_trans_detail where coalesce(last_status, 0)=0 and trans_flag in (239) and ic_trans.doc_no=ap_ar_trans_detail.billing_no \n"
                    + "                    and ic_trans.doc_date=ap_ar_trans_detail.billing_date)) as balance_amount,branch_code from ic_trans  where  coalesce(last_status, 0)=0 \n"
                    + "                    and ((trans_flag=48 and inquiry_type in (0,2,4) ) or trans_flag=97 or trans_flag=103)  and cust_code=ic_qt.cust_code ) as xx \n"
                    + "                    where balance_amount  <> 0 and doc_no = ap_ar.billing_no order by cust_code, doc_date, doc_no limit 1 ),0) as balance,\n"
                    + "                    case when ap_ar.doc_no is not null THEN 'success' when ap_ar.doc_no is null and ap_inv.doc_no is not null THEN 'payment' when  ap_ar.doc_no is null and ap_inv.doc_no is null and ap_so.doc_no is not null THEN 'packing' when ic_soc.doc_no is not null THEN 'cancel' else 'pending' end as status\n"
                    + "from ic_trans ic_qt left join \n"
                    + "ic_trans ic_soc on ic_soc.doc_ref = ic_qt.doc_no and ic_soc.trans_flag = 31 left join \n"
                    + "ap_ar_trans_detail ap_so on ap_so.billing_no = ic_qt.doc_no and ap_so.trans_flag = 36 left join \n"
                    + "ap_ar_trans_detail ap_inv on ap_inv.billing_no = ap_so.doc_no and ap_inv.trans_flag = 44  left join \n"
                    + "ic_trans ic_inv on ic_inv.doc_no = ap_inv.doc_no and ic_inv.trans_flag = 44  left join \n"
                    + "ap_ar_trans_detail ap_ar on ap_ar.billing_no = ap_inv.doc_no and ap_ar.trans_flag = 239 left join "
                    + "ic_trans_shipment its on its.doc_no = ic_qt.doc_no "
                    + "where ic_qt.trans_flag = 30 and ic_qt.cust_code = '" + strCust + "' and ic_qt.doc_no = '" + strDocNo + "' order by doc_date desc limit 1 ) as temp where 1=1 ";
            Statement __stmt1;
            ResultSet __rs1;
            System.out.println(__strQUERY1);
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            JSONObject obj = new JSONObject();
            while (__rs1.next()) {
                obj.put("remark_5", __rs1.getString("remark_5"));
                obj.put("doc_no", __rs1.getString("doc_no"));
                obj.put("doc_date", __rs1.getString("doc_date"));
                obj.put("doc_time", __rs1.getString("doc_time"));
                obj.put("cust_code", __rs1.getString("cust_code"));
                obj.put("send_type", __rs1.getString("send_type"));
                obj.put("total_amount", __rs1.getString("total_amount"));
                obj.put("emp_code", __rs1.getString("emp_code"));
                obj.put("emp_name", __rs1.getString("emp_name"));
                obj.put("balance", __rs1.getString("balance"));
                obj.put("address", __rs1.getString("address"));
                obj.put("telephone", __rs1.getString("telephone"));
                obj.put("address_name", __rs1.getString("address_name"));
                obj.put("total_except_vat", __rs1.getString("total_except_vat"));
                obj.put("total_after_vat", __rs1.getString("total_after_vat"));
                obj.put("total_vat_value", __rs1.getString("total_vat_value"));
                obj.put("items", new JSONArray());
                if (__rs1.getString("status").equals("success") && Float.parseFloat(__rs1.getString("balance")) > 0) {
                    obj.put("status", "partial");
                } else {
                    obj.put("status", __rs1.getString("status"));
                }
                String __strQUERYCount = "select item_code,item_name,qty,unit_code,price,wh_code,shelf_code,stand_value,divide_value,ratio,sum_amount,qty from ic_trans_detail where doc_no = '" + __rs1.getString("doc_no") + "'";
                Statement __stmtTotal = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet _rsTotal = __stmtTotal.executeQuery(__strQUERYCount);
                JSONArray _jsonArrx = new JSONArray();
                while (_rsTotal.next()) {

                    JSONObject objx = new JSONObject();
                    objx.put("qty", _rsTotal.getString("qty"));
                    objx.put("item_code", _rsTotal.getString("item_code"));
                    objx.put("item_name", _rsTotal.getString("item_name"));
                    objx.put("unit_code", _rsTotal.getString("unit_code"));
                    objx.put("wh_code", _rsTotal.getString("wh_code"));
                    objx.put("shelf_code", _rsTotal.getString("shelf_code"));
                    objx.put("stand_value", _rsTotal.getString("stand_value"));
                    objx.put("divide_value", _rsTotal.getString("divide_value"));
                    objx.put("ratio", _rsTotal.getString("ratio"));
                    objx.put("price", _rsTotal.getString("price"));
                    objx.put("sum_amount", _rsTotal.getString("sum_amount"));
                    _jsonArrx.put(objx);

                }
                _rsTotal.close();
                __stmtTotal.close();
                obj.put("items", _jsonArrx);
            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("data", obj);

            __objResponse.put("success", true);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getDocList")
    public Response getDocList(
            @QueryParam("cust_code") String strCust,
            @QueryParam("trans_flag") String strTrans
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";
            if (!strTrans.equals("")) {
                where = " and ic.trans_flag = " + strTrans;
            } else {
                where = " and ic.trans_flag in (40,44,48,46)";
            }
            // String __strQUERY1 = "select coalesce(remark,'') as remark ,trans_flag,doc_no,doc_date,doc_time,cust_code,send_type,total_amount,sale_code as emp_code,coalesce((select name_1 from erp_user where upper(code) = upper(sale_code) limit 1),'') as emp_name from ic_trans where cust_code = '" + strCust + "' and trans_flag = '" + strTrans + "' order by doc_date limit 50 ";
            String __strQUERY1 = "WITH SUM_CN AS (\n"
                    + "select b.ref_doc_no as ref_doc_no,\n"
                    + "sum(a.total_amount) as cn_total_amount,\n"
                    + "sum(a.total_except_vat) as cn_total_except_vat,\n"
                    + "sum(a.total_after_vat) as cn_total_after_vat,\n"
                    + "sum(a.total_vat_value) as cn_total_vat_value\n"
                    + "from ic_trans a\n"
                    + "left join ic_trans_detail b on a.doc_no=b.doc_no\n"
                    + "\n"
                    + "group by b.ref_doc_no\n"
                    + ")\n"
                    + "\n"
                    + "SELECT\n"
                    + "    ic.inquiry_type,\n"
                    + "    ic.last_status,\n"
                    + "    ic.trans_flag,\n"
                    + "    ic.send_type,\n"
                    + "    ic.doc_no,\n"
                    + "    ic.doc_date,\n"
                    + "    ic.cust_code,\n"
                    + "    ic.doc_time,\n"
                    + "    COALESCE((SELECT name_1 FROM erp_user WHERE UPPER(code) = UPPER(ic.sale_code) LIMIT 1), '') AS emp_name,\n"
                    + "    COALESCE(ic.sale_code, '') AS emp_code,\n"
                    + "    ic.remark,\n"
                    + "    ic.doc_group AS status,\n"
                    + "    COALESCE(ic.total_discount, 0) AS total_discount,\n"
                    + "    COALESCE(ic.discount_word, '') AS discount_word,\n"
                    + "    COALESCE((\n"
                    + "        SELECT SUM(balance_amount)\n"
                    + "        FROM (\n"
                    + "            SELECT cust_code, doc_date, credit_date AS due_date, doc_no, trans_flag AS doc_type, used_status,\n"
                    + "                   doc_ref AS ref_doc_no, doc_ref_date AS ref_doc_date,\n"
                    + "                   COALESCE(total_amount, 0) AS amount,\n"
                    + "                   COALESCE(total_amount, 0) - (\n"
                    + "                       SELECT COALESCE(SUM(COALESCE(sum_pay_money, 0)), 0)\n"
                    + "                       FROM ap_ar_trans_detail\n"
                    + "                       WHERE COALESCE(last_status, 0) = 0\n"
                    + "                         AND trans_flag IN (239)\n"
                    + "                         AND ic_trans.doc_no = ap_ar_trans_detail.billing_no\n"
                    + "                         AND ic_trans.doc_date = ap_ar_trans_detail.billing_date\n"
                    + "                   ) AS balance_amount,\n"
                    + "                   branch_code\n"
                    + "            FROM ic_trans\n"
                    + "            WHERE COALESCE(last_status, 0) = 0\n"
                    + "              AND trans_flag = 44\n"
                    + "              AND (inquiry_type = 0 OR inquiry_type = 2)\n"
                    + "              AND cust_code = ic.cust_code\n"
                    + "\n"
                    + "            UNION ALL\n"
                    + "\n"
                    + "            SELECT cust_code, doc_date, credit_date AS due_date, doc_no, trans_flag AS doc_type, used_status,\n"
                    + "                   '' AS ref_doc_no, NULL AS ref_doc_date,\n"
                    + "                   COALESCE(total_amount, 0) AS amount,\n"
                    + "                   COALESCE(total_amount, 0) - (\n"
                    + "                       SELECT COALESCE(SUM(COALESCE(sum_pay_money, 0)), 0)\n"
                    + "                       FROM ap_ar_trans_detail\n"
                    + "                       WHERE COALESCE(last_status, 0) = 0\n"
                    + "                         AND trans_flag IN (239)\n"
                    + "                         AND ic_trans.doc_no = ap_ar_trans_detail.billing_no\n"
                    + "                         AND ic_trans.doc_date = ap_ar_trans_detail.billing_date\n"
                    + "                   ) AS balance_amount,\n"
                    + "                   branch_code\n"
                    + "            FROM ic_trans\n"
                    + "            WHERE COALESCE(last_status, 0) = 0\n"
                    + "              AND trans_flag IN (46, 93, 99, 95, 101)\n"
                    + "              AND cust_code = ic.cust_code\n"
                    + "\n"
                    + "            UNION ALL\n"
                    + "\n"
                    + "            SELECT cust_code, doc_date, credit_date AS due_date, doc_no, trans_flag AS doc_type, used_status,\n"
                    + "                   '' AS ref_doc_no, NULL AS ref_doc_date,\n"
                    + "                   -1 * COALESCE(total_amount, 0) AS amount,\n"
                    + "                   -1 * (COALESCE(total_amount, 0) + (\n"
                    + "                       SELECT COALESCE(SUM(COALESCE(sum_pay_money, 0)), 0)\n"
                    + "                       FROM ap_ar_trans_detail\n"
                    + "                       WHERE COALESCE(last_status, 0) = 0\n"
                    + "                         AND trans_flag IN (239)\n"
                    + "                         AND ic_trans.doc_no = ap_ar_trans_detail.billing_no\n"
                    + "                         AND ic_trans.doc_date = ap_ar_trans_detail.billing_date\n"
                    + "                   )) AS balance_amount,\n"
                    + "                   branch_code\n"
                    + "            FROM ic_trans\n"
                    + "            WHERE COALESCE(last_status, 0) = 0\n"
                    + "              AND (\n"
                    + "                  (trans_flag = 48 AND inquiry_type IN (0, 2, 4)) OR\n"
                    + "                  trans_flag IN (97, 103)\n"
                    + "              )\n"
                    + "              AND cust_code = ic.cust_code\n"
                    + "        ) AS xx\n"
                    + "        WHERE balance_amount <> 0\n"
                    + "          AND doc_no = ar.doc_no\n"
                    + "  GROUP BY cust_code, doc_date, doc_no \n"
                    + "        ORDER BY cust_code, doc_date, doc_no\n"
                    + "    ), 0) AS balance,\n"
                    + "    COALESCE(cus.name_1, '') AS cust_name,\n"
                    + "    COALESCE(ar.doc_no, '') AS ar_no,\n"
                    + "    ic.total_amount-coalesce((select cn_total_amount from SUM_CN where ic.doc_no=SUM_CN.ref_doc_no),0) as total_amount,"
                    + "    coalesce((select cn_total_amount from SUM_CN where ic.doc_no=SUM_CN.ref_doc_no),0) as cn_total_amount "
                    + " FROM ic_trans ic\n"
                    + " LEFT JOIN ar_customer cus ON cus.code = ic.cust_code\n"
                    + " LEFT JOIN ap_ar_trans_detail ar ON ar.billing_no = ic.doc_no AND ar.trans_flag = 239\n"
                    + " WHERE 1 = 1 "
                    + "  AND ic.cust_code = '" + strCust + "' " + where
                    + " ORDER BY ic.create_datetime DESC, ic.doc_no ASC;";

            Statement __stmt1;
            ResultSet __rs1;
            System.out.println(__strQUERY1);
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();

            while (__rs1.next()) {
                JSONObject obj = new JSONObject();

                obj.put("inquiry_type", __rs1.getString("inquiry_type"));
                obj.put("trans_flag", __rs1.getString("trans_flag"));
                obj.put("last_status", __rs1.getString("last_status"));
                obj.put("doc_no", __rs1.getString("doc_no"));
                obj.put("doc_date", __rs1.getString("doc_date"));
                obj.put("doc_time", __rs1.getString("doc_time"));
                obj.put("cust_code", __rs1.getString("cust_code"));
                obj.put("send_type", __rs1.getString("send_type"));
                obj.put("total_amount", __rs1.getString("total_amount"));
                obj.put("emp_code", __rs1.getString("emp_code"));
                obj.put("emp_name", __rs1.getString("emp_name"));
                obj.put("remark", __rs1.getString("remark"));

                obj.put("trans_flag", __rs1.getString("trans_flag"));
                obj.put("doc_no", __rs1.getString("doc_no"));
                obj.put("ar_no", __rs1.getString("ar_no"));

                obj.put("doc_date", __rs1.getString("doc_date"));
                obj.put("cust_code", __rs1.getString("cust_code"));
                obj.put("doc_time", __rs1.getString("doc_time"));

                obj.put("total_discount", __rs1.getString("total_discount"));
                obj.put("balance", __rs1.getString("balance"));

                obj.put("ar_no", __rs1.getString("ar_no"));
                obj.put("discount_word", __rs1.getString("discount_word"));
                obj.put("status", __rs1.getString("status"));
                obj.put("cust_name", __rs1.getString("cust_name"));
                obj.put("cn_total_amount", __rs1.getString("cn_total_amount"));
                obj.put("total_amount", __rs1.getString("total_amount"));
                obj.put("remark", __rs1.getString("remark"));
                __jsonArr.put(obj);
            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("data", __jsonArr);

            __objResponse.put("success", true);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/sendApprove")
    public Response sendApprove(
            @QueryParam("docno") String strDocno,
            @QueryParam("docref") String strDocref,
            @QueryParam("empcode") String StrEmpcode
    ) {
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            String strProvider = "DEMO";
            String strDatabaseName = "demo1";
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));

            String __strQUERY0 = "insert into ic_trans ("
                    + "trans_type, trans_flag, " // smallint, smallint
                    + "doc_date, doc_time, doc_no,doc_ref, " // date, varchar(5), varchar(25)
                    + "inquiry_type, vat_type, " // smallint, smallint
                    + "cust_code, branch_code, vat_rate, "
                    + "user_request, approve_status, " // varchar(25), smallint
                    + "doc_format_code, " // varchar(25)
                    + "remark, creator_code, " // varchar(255), varchar(255), varchar(25)
                    + "create_datetime" // timestamp
                    + ") select 1, 4, " // smallint, smallint
                    + "doc_date, doc_time,'" + strDocno + "', '" + strDocref + "', " // date, varchar(5), varchar(25)
                    + "inquiry_type, vat_type, " // smallint, smallint
                    + "cust_code, branch_code, vat_rate, "
                    + "user_request, 0, " // varchar(25), smallint
                    + "'PRA', " // varchar(25)
                    + "remark, '" + StrEmpcode + "', " // varchar(255), varchar(255), varchar(25)
                    + "'now()' from ic_trans WHERE doc_no ='" + strDocref + "' and trans_flag = 2 ;";
            System.out.println("__strQUERY0" + __strQUERY0);
            Statement __stmt0;

            __stmt0 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __stmt0.executeUpdate(__strQUERY0);
            __stmt0.close();

            String __strQUERYdetail = "insert into ic_trans_detail ("
                    + "trans_type, trans_flag, " // smallint, smallint
                    + "doc_date, doc_time, doc_no, " // date, varchar(5), varchar(25)
                    + "cust_code, " // varchar(25)
                    + "item_code, item_name, unit_code, " // varchar(25), varchar(255), varchar(25)
                    + "qty, price, sum_amount, " // numeric x3
                    + "line_number, " // integer
                    + "stand_value, divide_value, ratio, " // numeric x3
                    + "calc_flag, vat_type, " // integer, integer
                    + "doc_date_calc, doc_time_calc, " // date, varchar(5)
                    + "create_datetime) select 1, 4, " // smallint, smallint
                    + "doc_date, doc_time,'" + strDocno + "', " // date, varchar(5), varchar(25)
                    + "cust_code, item_code, item_name, unit_code, " // varchar(25), varchar(255), varchar(25)
                    + "qty, price, sum_amount, " // numeric x3
                    + "line_number, " // integer
                    + "stand_value, divide_value, ratio, " // numeric x3
                    + "1, vat_type, " // integer, integer
                    + "doc_date_calc, doc_time_calc, " // date, varchar(5)
                    + "'now()' from ic_trans_detail WHERE doc_no ='" + strDocref + "' ;";
            System.out.println("__strQUERYdetail " + __strQUERYdetail);
            Statement __stmtdetail;

            __stmtdetail = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __stmtdetail.executeUpdate(__strQUERYdetail);
            __stmtdetail.close();

            String __strQUERY1 = "update ic_trans set approve_status=1,approve_code='" + StrEmpcode + "',approve_date='now()',doc_success=1,used_status=1 WHERE doc_no ='" + strDocref + "' ;";
            System.out.println("__strQUERY1");
            Statement __stmt1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __stmt1.executeUpdate(__strQUERY1);
            __stmt1.close();

            __conn.close();

            __objResponse.put("success", true);
            __objResponse.put("data", new JSONArray());
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/cancelApprove")
    public Response cancelApprove(
            @QueryParam("docno") String strDocno,
            @QueryParam("docref") String strDocref,
            @QueryParam("empcode") String StrEmpcode) {
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            String strProvider = "DEMO";
            String strDatabaseName = "demo1";
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));

            String __strQUERY1 = "update ic_trans set approve_status=2,approve_code='" + StrEmpcode + "',doc_success=1,used_status=1,approve_date='now()' WHERE doc_no ='" + strDocref + "' ;";
            System.out.println("__strQUERY1");
            Statement __stmt1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __stmt1.executeUpdate(__strQUERY1);
            __stmt1.close();
            __conn.close();

            __objResponse.put("success", true);
            __objResponse.put("data", new JSONArray());
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getPRDocWaitApprove")
    public Response getPRDocWaitApprove(
            @QueryParam("search") String strSearch,
            @QueryParam("fromdate") String strFromDate,
            @QueryParam("todate") String strTodate
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";

            if (!strFromDate.equals("") && !strTodate.equals("")) {
                where += " and doc_date between '" + strFromDate + "' and '" + strTodate + "' ";
            }
            if (!strSearch.equals("")) {
                where = "";
                where += " and (doc_no like '%" + strSearch + "%' or cust_code like '%" + strSearch + "%' or user_request like '%" + strSearch + "%') ";
            }

            // String __strQUERY1 = "select coalesce(remark,'') as remark ,trans_flag,doc_no,doc_date,doc_time,cust_code,send_type,total_amount,sale_code as emp_code,coalesce((select name_1 from erp_user where upper(code) = upper(sale_code) limit 1),'') as emp_name from ic_trans where cust_code = '" + strCust + "' and trans_flag = '" + strTrans + "' order by doc_date limit 50 ";
            String __strQUERY1 = "select doc_date,doc_time,doc_no,cust_code,user_request,approve_status,remark,creator_code from ic_trans where doc_format_code = 'PR' and last_status = 0 and approve_status = 0 and doc_success = 0 " + where + " order by doc_date desc,doc_time desc limit 100";

            Statement __stmt1;
            ResultSet __rs1;
            System.out.println(__strQUERY1);
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();

            while (__rs1.next()) {
                JSONObject obj = new JSONObject();

                obj.put("doc_no", __rs1.getString("doc_no"));
                obj.put("doc_date", __rs1.getString("doc_date"));
                obj.put("doc_time", __rs1.getString("doc_time"));
                obj.put("cust_code", __rs1.getString("cust_code"));
                obj.put("user_request", __rs1.getString("user_request"));
                obj.put("remark", __rs1.getString("remark"));

                __jsonArr.put(obj);
            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("data", __jsonArr);

            __objResponse.put("success", true);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getPRDocApproved")
    public Response getPRDocApproved(
            @QueryParam("search") String strSearch,
            @QueryParam("fromdate") String strFromDate,
            @QueryParam("todate") String strTodate
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";

            if (!strFromDate.equals("") && !strTodate.equals("")) {
                where += " and doc_date between '" + strFromDate + "' and '" + strTodate + "' ";
            }
            if (!strSearch.equals("")) {
                where = "";
                where += " and (doc_no like '%" + strSearch + "%' or cust_code like '%" + strSearch + "%' or user_request like '%" + strSearch + "%') ";
            }

            // String __strQUERY1 = "select coalesce(remark,'') as remark ,trans_flag,doc_no,doc_date,doc_time,cust_code,send_type,total_amount,sale_code as emp_code,coalesce((select name_1 from erp_user where upper(code) = upper(sale_code) limit 1),'') as emp_name from ic_trans where cust_code = '" + strCust + "' and trans_flag = '" + strTrans + "' order by doc_date limit 50 ";
            String __strQUERY1 = "select doc_date,doc_time,doc_no,cust_code,user_request,remark,creator_code from ic_trans where doc_format_code = 'PRA' and trans_flag = 4 and last_status = 0 and doc_success = 0 " + where + " order by doc_date,doc_time desc desc limit 100";

            Statement __stmt1;
            ResultSet __rs1;
            System.out.println(__strQUERY1);
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();

            while (__rs1.next()) {
                JSONObject obj = new JSONObject();

                obj.put("doc_no", __rs1.getString("doc_no"));
                obj.put("doc_date", __rs1.getString("doc_date"));
                obj.put("doc_time", __rs1.getString("doc_time"));
                obj.put("cust_code", __rs1.getString("cust_code"));
                obj.put("approve_code", __rs1.getString("user_request"));
                obj.put("remark", __rs1.getString("remark"));

                __jsonArr.put(obj);
            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("data", __jsonArr);

            __objResponse.put("success", true);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getPODocList")
    public Response getPODocList(
            @QueryParam("search") String strSearch,
            @QueryParam("fromdate") String strFromDate,
            @QueryParam("todate") String strTodate
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";

            if (!strFromDate.equals("") && !strTodate.equals("")) {
                where += " and doc_date between '" + strFromDate + "' and '" + strTodate + "' ";
            }
            if (!strSearch.equals("")) {
                where = "";
                where += " and (doc_no like '%" + strSearch + "%' or cust_code like '%" + strSearch + "%' or user_request like '%" + strSearch + "%') ";
            }

            // String __strQUERY1 = "select coalesce(remark,'') as remark ,trans_flag,doc_no,doc_date,doc_time,cust_code,send_type,total_amount,sale_code as emp_code,coalesce((select name_1 from erp_user where upper(code) = upper(sale_code) limit 1),'') as emp_name from ic_trans where cust_code = '" + strCust + "' and trans_flag = '" + strTrans + "' order by doc_date limit 50 ";
            String __strQUERY1 = "select trans_type,trans_flag,doc_date,doc_time,doc_no,inquiry_type,vat_type,cust_code,vat_rate,user_request,doc_format_code,creator_code,remark,"
                    + "  (\n"
                    + "    SELECT ARRAY_TO_STRING(\n"
                    + "      ARRAY(\n"
                    + "        SELECT billing_no \n"
                    + "        FROM ap_ar_trans_detail \n"
                    + "        WHERE ap_ar_trans_detail.doc_no = ic_trans.doc_no\n"
                    + "      ), ','\n"
                    + "    )\n"
                    + "  ) AS pra_doc_list from ic_trans where trans_flag=6 and last_status = 0 " + where + " order by doc_date desc,doc_time desc limit 100";

            Statement __stmt1;
            ResultSet __rs1;
            System.out.println(__strQUERY1);
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();

            while (__rs1.next()) {
                JSONObject obj = new JSONObject();

                obj.put("doc_no", __rs1.getString("doc_no"));
                obj.put("doc_date", __rs1.getString("doc_date"));
                obj.put("doc_time", __rs1.getString("doc_time"));
                obj.put("cust_code", __rs1.getString("cust_code"));
                obj.put("user_request", __rs1.getString("user_request"));
                obj.put("creator_code", __rs1.getString("creator_code"));
                obj.put("pra_doc_list", __rs1.getString("pra_doc_list"));
                obj.put("remark", __rs1.getString("remark"));

                __jsonArr.put(obj);
            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("data", __jsonArr);

            __objResponse.put("success", true);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @POST
    @Path("/updatePoDetail")
    public Response updatePoDetail(
            @QueryParam("docno") String strDocNo,
            String data
    ) throws Exception {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();

        __objResponse.put("success", false);
        try {
            String _guid_code = "";
            String _item_code = "";
            String _item_name = "";
            String _unit_code = "";
            String _barcode = "";
            String _qty = "";
            String _price = "";
            String _wh_code = "";
            String _shelf_code = "";
            String __cust_code = "";
            String __creator_code = "";
            String _stand_value = "";
            String _divide_value = "";
            String _ratio = "";
            String _doc_no = "";
            String _doc_date = "";
            String _doc_time = "";
            if (data != null) {

                JSONArray objJSArr = new JSONArray(data);

                StringBuilder __query_builder = new StringBuilder();
                UUID uuid = UUID.randomUUID();
                String strGUID = uuid.toString();

                _routine __routine = new _routine();
                Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));

                String __deleteQuery = "delete from ic_trans_detail where doc_no = '" + strDocNo + "'";
                System.out.println("__deleteQuery " + __deleteQuery.toString());

                Statement __stmtdelete = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                __stmtdelete.executeUpdate(__deleteQuery.toString());
                __stmtdelete.close();

                for (int i = 0; i < objJSArr.length(); i++) {
                    JSONObject objJSData = objJSArr.getJSONObject(i);
                    _doc_no = objJSData.has("doc_no") ? objJSData.getString("doc_no") : "";
                    _doc_date = objJSData.has("doc_date") ? objJSData.getString("doc_date") : "";
                    _doc_time = objJSData.has("doc_time") ? objJSData.getString("doc_time") : "";
                    __cust_code = objJSData.has("cust_code") ? objJSData.getString("cust_code") : "";
                    __creator_code = objJSData.has("emp_code") ? objJSData.getString("emp_code") : "";
                    _item_code = objJSData.has("item_code") ? objJSData.getString("item_code") : "";
                    _item_name = objJSData.has("item_name") ? objJSData.getString("item_name") : "";
                    _unit_code = objJSData.has("unit_code") ? objJSData.getString("unit_code") : "";
                    _barcode = objJSData.has("barcode") ? objJSData.getString("barcode") : "";
                    _qty = objJSData.has("qty") ? objJSData.getString("qty") : "1";
                    _price = objJSData.has("price") ? objJSData.getString("price") : "0";
                    _stand_value = objJSData.has("stand_value") ? objJSData.getString("stand_value") : "1";
                    _divide_value = objJSData.has("divide_value") ? objJSData.getString("divide_value") : "1";
                    _ratio = objJSData.has("ratio") ? objJSData.getString("ratio") : "1";

                    __query_builder.append("insert into ic_trans_detail (trans_type, trans_flag,vat_type, " // smallint, smallint
                            + "doc_date, doc_time, doc_no, " // date, varchar(5), varchar(25)
                            + "cust_code,item_code,item_name,unit_code,barcode,qty,price,creator_code,create_datetime,stand_value,divide_value,ratio,line_number,calc_flag,sum_amount,doc_date_calc, doc_time_calc) values (1,6,1,'" + _doc_date + "','" + _doc_time + "','" + _doc_no + "','" + __cust_code + "','" + _item_code + "','" + _item_name + "','" + _unit_code + "','" + _barcode + "'"
                            + ",'" + _qty + "','" + _price + "','" + __creator_code + "','now()','" + _stand_value + "','" + _divide_value + "','" + _ratio + "','" + i + "',1,'" + (Float.parseFloat(_qty) * Float.parseFloat(_price)) + "','" + _doc_date + "','" + _doc_time + "');");

                }
                Statement __stmt2;

                System.out.println("__query_builder " + __query_builder);

                __stmt2 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                __stmt2.executeUpdate(__query_builder.toString());
                __objResponse.put("msg", "success");
                __objResponse.put("success", true);
                __stmt2.close();
                __conn.close();

            } else {
                return Response.status(400).entity("{ERROR: Data is null}").build();
            }

        } catch (JSONException ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(__objResponse.toString(), MediaType.APPLICATION_JSON).build();
    }

    @POST
    @Path("/createPoDoc")
    public Response createPoDoc(String data) throws Exception {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);

        try {
            if (data == null) {
                return Response.status(400).entity("{ERROR: Data is null}").build();
            }

            JSONObject objJSData = new JSONObject(data);

            String doc_no = objJSData.optString("doc_no", "");
            String doc_time = objJSData.optString("doc_time", "");
            String cust_code = objJSData.optString("cust_code", "");
            String doc_date = objJSData.optString("doc_date", "");
            String branch_code = objJSData.optString("branch_code", "");
            String vat_rate = "7";
            String creator_code = objJSData.optString("emp_code", "");
            String remark = objJSData.optString("remark", "");
            BigDecimal totalValue = getBigDecimal(objJSData, "total_value", BigDecimal.ZERO);
            BigDecimal totalExceptVat = getBigDecimal(objJSData, "total_except_vat", BigDecimal.ZERO);
            BigDecimal totalAfterVat = getBigDecimal(objJSData, "total_after_vat", BigDecimal.ZERO);
            BigDecimal totalAmount = getBigDecimal(objJSData, "total_amount", BigDecimal.ZERO);

            // คำนวณ total_before_vat, total_vat_value (ใช้ BigDecimal แทน Float เพื่อความแม่นยำ)
            BigDecimal totalBeforeVat = totalAfterVat.multiply(new BigDecimal("100"))
                    .divide(new BigDecimal("107"), 4, RoundingMode.HALF_UP);
            BigDecimal totalVatValue = totalAfterVat.subtract(totalBeforeVat);

            // BUG FIX #1: has("doc_detail") แต่ดึง "doc_list" → ใช้ key เดียวกัน
            JSONArray objJSArrDoc = objJSData.has("doc_list") ? objJSData.getJSONArray("doc_list") : new JSONArray();
            JSONArray objJSArrItems = objJSData.has("items") ? objJSData.getJSONArray("items") : new JSONArray();

            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));
            __conn.setAutoCommit(false);

            try {
                // --- 1) ap_ar_trans_detail INSERT ---
                // BUG FIX #2: ลบ "1," ที่เกินออก (column 7 ค่า แต่ values เดิมมี 8 ค่า)
                String sqlApAr = "INSERT INTO ap_ar_trans_detail "
                        + "(trans_type, trans_flag, doc_date, doc_no, billing_no, billing_date, calc_flag) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?)";

                // --- 2) ic_trans UPDATE ---
                String sqlUpdateIcTrans = "UPDATE ic_trans SET doc_success = 1, used_status = 1 WHERE doc_no = ?";

                PreparedStatement __stmtApAr = __conn.prepareStatement(sqlApAr);
                PreparedStatement __stmtUpdateIcTrans = __conn.prepareStatement(sqlUpdateIcTrans);

                for (int i = 0; i < objJSArrDoc.length(); i++) {
                    JSONObject objJSDataDoc = objJSArrDoc.getJSONObject(i);
                    String _doc_no = objJSDataDoc.optString("doc_no", "");
                    String _doc_date = objJSDataDoc.optString("doc_date", "");

                    // INSERT ap_ar_trans_detail
                    __stmtApAr.setInt(1, 1);
                    __stmtApAr.setInt(2, 6);
                    __stmtApAr.setDate(3, toSqlDate(doc_date));
                    __stmtApAr.setString(4, doc_no);
                    __stmtApAr.setString(5, _doc_no);
                    __stmtApAr.setDate(6, toSqlDate(_doc_date));
                    __stmtApAr.setInt(7, 1);
                    __stmtApAr.addBatch();

                    // UPDATE ic_trans
                    __stmtUpdateIcTrans.setString(1, _doc_no);
                    __stmtUpdateIcTrans.addBatch();
                }

                __stmtApAr.executeBatch();
                __stmtApAr.close();
                __stmtUpdateIcTrans.executeBatch();
                __stmtUpdateIcTrans.close();

                // --- 3) ic_trans_detail INSERT ---
                // BUG FIX #3: ลบ "1," ที่เกินออกหลัง trans_flag (values เดิมมีค่าเกิน 1 ค่า)
                String sqlDetail = "INSERT INTO ic_trans_detail "
                        + "(trans_type, trans_flag, doc_date, doc_time, doc_no, cust_code, inquiry_type, "
                        + " item_code, item_name, unit_code, qty, price, sum_amount, line_number, "
                        + " stand_value, divide_value, ratio, calc_flag, doc_date_calc, doc_time_calc, creator_code) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                PreparedStatement __stmtItems = __conn.prepareStatement(sqlDetail);
                for (int i = 0; i < objJSArrItems.length(); i++) {
                    JSONObject item = objJSArrItems.getJSONObject(i);
                    __stmtItems.setInt(1, 1);
                    __stmtItems.setInt(2, 6);
                    __stmtItems.setDate(3, toSqlDate(doc_date));
                    __stmtItems.setString(4, doc_time);
                    __stmtItems.setString(5, doc_no);
                    __stmtItems.setString(6, cust_code);
                    __stmtItems.setInt(7, 0);
                    __stmtItems.setString(8, item.getString("item_code"));
                    __stmtItems.setString(9, item.getString("item_name"));
                    __stmtItems.setString(10, item.getString("unit_code"));
                    __stmtItems.setBigDecimal(11, toDecimal(item.getString("qty")));          // ✅ numeric
                    __stmtItems.setBigDecimal(12, toDecimal(item.getString("price")));        // ✅ numeric
                    __stmtItems.setBigDecimal(13, toDecimal(item.getString("sum_amount")));   // ✅ numeric
                    __stmtItems.setInt(14, i);
                    __stmtItems.setBigDecimal(15, toDecimal(item.getString("stand_value"))); // ✅ numeric
                    __stmtItems.setBigDecimal(16, toDecimal(item.getString("divide_value")));// ✅ numeric
                    __stmtItems.setBigDecimal(17, toDecimal(item.getString("ratio")));
                    __stmtItems.setInt(18, 1);
                    __stmtItems.setDate(19, toSqlDate(doc_date));
                    __stmtItems.setString(20, doc_time);
                    __stmtItems.setString(21, creator_code);
                    __stmtItems.addBatch();
                }
                __stmtItems.executeBatch();
                __stmtItems.close();

                // --- 4) ic_trans INSERT ---
                // BUG FIX #4: เพิ่ม VALUES ที่หายไป
                String sqlPoTrans = "INSERT INTO ic_trans "
                        + "(trans_type, trans_flag, doc_date, doc_time, doc_no, inquiry_type, vat_type, "
                        + " cust_code, vat_rate, user_request, doc_format_code, creator_code, remark, branch_code,total_value, total_before_vat, total_vat_value, " // numeric x3
                        + "total_after_vat, total_except_vat, " // numeric x2
                        + "total_amount, balance_amount) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?)";

                PreparedStatement __stmtPoTrans = __conn.prepareStatement(sqlPoTrans);
                __stmtPoTrans.setInt(1, 1);
                __stmtPoTrans.setInt(2, 6);
                __stmtPoTrans.setDate(3, toSqlDate(doc_date));
                __stmtPoTrans.setString(4, doc_time);
                __stmtPoTrans.setString(5, doc_no);
                __stmtPoTrans.setInt(6, 0);
                __stmtPoTrans.setInt(7, 1);
                __stmtPoTrans.setString(8, cust_code);
                __stmtPoTrans.setBigDecimal(9, toDecimal(vat_rate));
                __stmtPoTrans.setString(10, creator_code);
                __stmtPoTrans.setString(11, "PO");
                __stmtPoTrans.setString(12, creator_code);
                __stmtPoTrans.setString(13, remark);
                __stmtPoTrans.setString(14, branch_code);
                __stmtPoTrans.setBigDecimal(15, totalValue);         // total_value       numeric
                __stmtPoTrans.setBigDecimal(16, totalBeforeVat);     // total_before_vat  numeric
                __stmtPoTrans.setBigDecimal(17, totalVatValue);      // total_vat_value   numeric
                __stmtPoTrans.setBigDecimal(18, totalAfterVat);      // total_after_vat   numeric
                __stmtPoTrans.setBigDecimal(19, totalExceptVat);     // total_except_vat  numeric
                __stmtPoTrans.setBigDecimal(20, totalAmount);        // total_amount      numeric
                __stmtPoTrans.setBigDecimal(21, totalAmount);        // balance_amount    numeric (= total_amount)
                __stmtPoTrans.executeUpdate();
                __stmtPoTrans.close();

                __conn.commit();
                __objResponse.put("msg", "success");
                __objResponse.put("success", true);

            } catch (Exception ex) {
                __conn.rollback();
                throw ex;
            } finally {
                __conn.close();
            }

        } catch (JSONException ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }

        return Response.ok(__objResponse.toString(), MediaType.APPLICATION_JSON).build();
    }

    // helper method แปลง String → BigDecimal
    private java.math.BigDecimal toDecimal(String val) {
        try {
            return new java.math.BigDecimal(val);
        } catch (Exception e) {
            return java.math.BigDecimal.ZERO;
        }
    }

    private java.sql.Date toSqlDate(String dateStr) {
        try {
            return java.sql.Date.valueOf(dateStr); // รับ format "yyyy-MM-dd"
        } catch (Exception e) {
            return null;
        }
    }

    @GET
    @Path("/getPRDocList")
    public Response getPRDocList(
            @QueryParam("search") String strSearch,
            @QueryParam("fromdate") String strFromDate,
            @QueryParam("todate") String strTodate
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";

            if (!strFromDate.equals("") && !strTodate.equals("")) {
                where += " and doc_date between '" + strFromDate + "' and '" + strTodate + "' ";
            }
            if (!strSearch.equals("")) {
                where = "";
                where += " and (doc_no like '%" + strSearch + "%' or cust_code like '%" + strSearch + "%' or user_request like '%" + strSearch + "%') ";
            }

            // String __strQUERY1 = "select coalesce(remark,'') as remark ,trans_flag,doc_no,doc_date,doc_time,cust_code,send_type,total_amount,sale_code as emp_code,coalesce((select name_1 from erp_user where upper(code) = upper(sale_code) limit 1),'') as emp_name from ic_trans where cust_code = '" + strCust + "' and trans_flag = '" + strTrans + "' order by doc_date limit 50 ";
            String __strQUERY1 = "select doc_date,doc_time,doc_no,cust_code,user_request,approve_status,remark,creator_code,approve_code,coalesce((select doc_no from ic_trans icx where icx.doc_ref = ic.doc_no and icx.trans_flag=4),'') as doc_ref,coalesce((select create_datetime from ic_trans icx where icx.doc_ref = ic.doc_no and icx.trans_flag=4),now()) as approve_datetime from ic_trans ic where doc_format_code = 'PR' and last_status = 0 " + where + " order by doc_date desc,doc_time desc limit 100";

            Statement __stmt1;
            ResultSet __rs1;
            System.out.println(__strQUERY1);
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();

            while (__rs1.next()) {
                JSONObject obj = new JSONObject();

                obj.put("doc_no", __rs1.getString("doc_no"));
                obj.put("doc_date", __rs1.getString("doc_date"));
                obj.put("doc_time", __rs1.getString("doc_time"));
                obj.put("cust_code", __rs1.getString("cust_code"));
                obj.put("user_request", __rs1.getString("user_request"));
                obj.put("approve_status", __rs1.getString("approve_status"));
                obj.put("approve_code", __rs1.getString("approve_code"));
                obj.put("doc_ref", __rs1.getString("doc_ref"));
                obj.put("approve_datetime", __rs1.getString("approve_datetime"));
                obj.put("remark", __rs1.getString("remark"));

                __jsonArr.put(obj);
            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("data", __jsonArr);

            __objResponse.put("success", true);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getTotalBalance")
    public Response getTotalBalance(
            @QueryParam("cust_code") String strCust
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";

            // String __strQUERY1 = "select coalesce(remark,'') as remark ,trans_flag,doc_no,doc_date,doc_time,cust_code,send_type,total_amount,sale_code as emp_code,coalesce((select name_1 from erp_user where upper(code) = upper(sale_code) limit 1),'') as emp_name from ic_trans where cust_code = '" + strCust + "' and trans_flag = '" + strTrans + "' order by doc_date limit 50 ";
            String __strQUERY1 = "select \n"
                    + "ar_balance as total_balance \n"
                    + "from (\n"
                    + "select \n"
                    + "sum(balance_amount) as ar_balance\n"
                    + "from (select cust_code\n"
                    + ",doc_date\n"
                    + ",due_date\n"
                    + ",doc_no\n"
                    + ",trans_flag(trans_flag) as doc_type\n"
                    + ",used_status\n"
                    + ",doc_ref as ref_doc_no\n"
                    + ",doc_ref_date as ref_doc_date\n"
                    + ",coalesce(total_amount,0) as amount,coalesce(total_amount,0)-(select coalesce(sum(coalesce(sum_pay_money,0)),0) from ap_ar_trans_detail where coalesce(last_status, 0)=0 and trans_flag in (239) and ic_trans.doc_no=ap_ar_trans_detail.billing_no and ic_trans.doc_date=ap_ar_trans_detail.billing_date and doc_date <= date(now())) as balance_amount from ic_trans where coalesce(last_status, 0)=0 and trans_flag=44 and (inquiry_type=0  or inquiry_type=2)  and doc_date <= date(now()) \n"
                    + "union all select cust_code,doc_date,due_date,doc_no,trans_flag(trans_flag) as doc_type,used_status,'' as ref_doc_no,null as ref_doc_date,coalesce(total_amount,0) as amount,coalesce(total_amount,0)-(select coalesce(sum(coalesce(sum_pay_money,0)),0) from ap_ar_trans_detail where coalesce(last_status, 0)=0 and trans_flag in (239) and ic_trans.doc_no=ap_ar_trans_detail.billing_no and ic_trans.doc_date=ap_ar_trans_detail.billing_date and doc_date <= date(now())) as balance_amount from ic_trans where coalesce(last_status, 0)=0 and (trans_flag=46 or trans_flag=93 or trans_flag=99 or trans_flag=95 or trans_flag=101) and doc_date <= date(now()) \n"
                    + "union all select cust_code,doc_date,due_date,doc_no,trans_flag(trans_flag) as doc_type,used_status,'' as ref_doc_no,null as ref_doc_date,-1*coalesce(total_amount,0) as amount,-1*(coalesce(total_amount,0)+(select coalesce(sum(coalesce(sum_pay_money,0)),0) from ap_ar_trans_detail where coalesce(last_status, 0)=0 and trans_flag in (239) and ic_trans.doc_no=ap_ar_trans_detail.billing_no and ic_trans.doc_date=ap_ar_trans_detail.billing_date and doc_date <= date(now()))) as balance_amount from ic_trans where coalesce(last_status, 0)=0 and ((trans_flag=48 and inquiry_type in (0,2,4) ) or trans_flag=97 or trans_flag=103) and doc_date <= date(now())\n"
                    + ") as temp2  where doc_no <> '' and cust_code = '" + strCust + "' \n"
                    + ") as temp3  where ar_balance<>0 ;";

            Statement __stmt1;
            ResultSet __rs1;
            System.out.println(__strQUERY1);
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            float total_balance = 0;
            while (__rs1.next()) {

                total_balance += __rs1.getFloat("total_balance");
            }

            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("total_balance", total_balance);

            __objResponse.put("success", true);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @POST
    @Path("/getPRADetail")
    public Response getPRADetail(
            String data
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));

            JSONArray __jsonArr = new JSONArray();
            JSONObject obj = new JSONObject();
            if (data != null) {

                JSONArray objJSArr = new JSONArray(data);

                String docnos = "";
                for (int i = 0; i < objJSArr.length(); i++) {
                    JSONObject objJSData = objJSArr.getJSONObject(i);
                    if (i != 0) {
                        docnos += ",'" + objJSData.getString("doc_no") + "'";
                    } else {
                        docnos += "'" + objJSData.getString("doc_no") + "'";
                    }
                }

                String __strQUERYCount = "SELECT \n"
                        + "  item_code,\n"
                        + "  item_name,\n"
                        + "  unit_code,\n"
                        + "  SUM(qty) AS sum_qty,\n"
                        + "  MAX(price) AS max_price,\n"
                        + "  SUM(qty) *   MAX(price) as sum_amount,\n"
                        + "  wh_code,\n"
                        + "  shelf_code,\n"
                        + "  stand_value,\n"
                        + "  divide_value,\n"
                        + "  ratio,\n"
                        + "  COALESCE((SELECT maximum_qty FROM ic_inventory_detail WHERE ic_code = item_code), 0) AS maximum_qty,\n"
                        + "  COALESCE((SELECT minimum_qty FROM ic_inventory_detail WHERE ic_code = item_code), 0) AS minimum_qty\n"
                        + "FROM ic_trans_detail \n"
                        + "WHERE doc_no IN (" + docnos + ")\n"
                        + "GROUP BY item_code, item_name, unit_code, wh_code, shelf_code, stand_value, divide_value, ratio;";
                System.out.println("__strQUERYCount" + __strQUERYCount);
                Statement __stmtTotal = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet _rsTotal = __stmtTotal.executeQuery(__strQUERYCount);
                JSONArray _jsonArrx = new JSONArray();
                while (_rsTotal.next()) {

                    JSONObject objx = new JSONObject();
                    objx.put("item_code", _rsTotal.getString("item_code"));
                    objx.put("item_name", _rsTotal.getString("item_name"));
                    objx.put("unit_code", _rsTotal.getString("unit_code"));
                    objx.put("minimum_qty", _rsTotal.getString("minimum_qty"));
                    objx.put("maximum_qty", _rsTotal.getString("maximum_qty"));
                    objx.put("qty", _rsTotal.getString("sum_qty"));
                    objx.put("stand_value", _rsTotal.getString("stand_value"));
                    objx.put("divide_value", _rsTotal.getString("divide_value"));
                    objx.put("ratio", _rsTotal.getString("ratio"));
                    objx.put("price", _rsTotal.getString("max_price"));
                    objx.put("sum_amount", _rsTotal.getString("sum_amount"));
                    _jsonArrx.put(objx);

                }
                _rsTotal.close();
                __stmtTotal.close();
                obj.put("items", _jsonArrx);

                __conn.close();
                __objResponse.put("data", obj);

                __objResponse.put("success", true);
            } else {
                return Response.status(400).entity("{ERROR: datanull}").build();
            }
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getDocDetail")
    public Response getDocDetail(
            @QueryParam("doc_no") String strDocNo
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));

            String __strQUERY1 = "select *,creator_code as emp_code,coalesce((select name_1 from erp_user where upper(code) = upper(creator_code) limit 1),'') as emp_name from ic_trans where  doc_no = '" + strDocNo + "' ";
            Statement __stmt1;
            ResultSet __rs1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            JSONObject obj = new JSONObject();
            while (__rs1.next()) {

                obj.put("remark", __rs1.getString("remark"));
                obj.put("doc_no", __rs1.getString("doc_no"));
                obj.put("doc_date", __rs1.getString("doc_date"));
                obj.put("doc_time", __rs1.getString("doc_time"));
                obj.put("cust_code", __rs1.getString("cust_code"));
                obj.put("send_type", __rs1.getString("send_type"));
                obj.put("total_amount", __rs1.getString("total_amount"));
                obj.put("emp_code", __rs1.getString("emp_code"));
                obj.put("emp_name", __rs1.getString("emp_name"));
                obj.put("items", new JSONArray());

                String __strQUERYCount = "select qty,item_code,item_name,qty,unit_code,price,wh_code,shelf_code,stand_value,divide_value,ratio,sum_amount,coalesce((select maximum_qty from ic_inventory_detail where ic_code = item_code ),0) as maximum_qty,coalesce((select minimum_qty from ic_inventory_detail where ic_code = item_code ),0) as minimum_qty  from ic_trans_detail where doc_no = '" + __rs1.getString("doc_no") + "'";
                Statement __stmtTotal = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet _rsTotal = __stmtTotal.executeQuery(__strQUERYCount);
                JSONArray _jsonArrx = new JSONArray();
                while (_rsTotal.next()) {

                    JSONObject objx = new JSONObject();
                    objx.put("item_code", _rsTotal.getString("item_code"));
                    objx.put("item_name", _rsTotal.getString("item_name"));
                    objx.put("unit_code", _rsTotal.getString("unit_code"));
                    objx.put("minimum_qty", _rsTotal.getString("minimum_qty"));
                    objx.put("maximum_qty", _rsTotal.getString("maximum_qty"));
                    objx.put("wh_code", _rsTotal.getString("wh_code"));
                    objx.put("shelf_code", _rsTotal.getString("shelf_code"));
                    objx.put("stand_value", _rsTotal.getString("stand_value"));
                    objx.put("qty", _rsTotal.getString("qty"));
                    objx.put("divide_value", _rsTotal.getString("divide_value"));
                    objx.put("ratio", _rsTotal.getString("ratio"));
                    objx.put("price", _rsTotal.getString("price"));
                    objx.put("sum_amount", _rsTotal.getString("sum_amount"));
                    _jsonArrx.put(objx);

                }
                _rsTotal.close();
                __stmtTotal.close();
                obj.put("items", _jsonArrx);
            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("data", obj);

            __objResponse.put("success", true);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getProductList")
    public Response getProductList(
            @QueryParam("cust_code") String strCustCode,
            @QueryParam("search") String strSearch,
            @QueryParam("category") String strCategory,
            @QueryParam("offset") String strOffset,
            @QueryParam("premium") String strPremium,
            @QueryParam("isstock") String strStock,
            @QueryParam("favorite") String strFavorite,
            @QueryParam("group") String strGroup,
            @QueryParam("groupsub") String strGroupSub,
            @QueryParam("groupsub2") String strGroupSub2,
            @QueryParam("brand") String strBrand,
            @QueryParam("category2") String strCategory2,
            @QueryParam("design") String strDesign,
            @QueryParam("model") String strModel,
            @QueryParam("limit") String strLimit
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));

            String _where = "";
            StringBuilder __where = new StringBuilder();
            String _whereFinal = "";

            String stockQtyExpr
                    = "((select balance_qty from ic_inventory where code=b.code) / "
                    + " NULLIF( ((select unit_standard_stand_value from ic_inventory where code=b.code) / "
                    + "         NULLIF((select unit_standard_divide_value from ic_inventory where code=b.code),0) ), 0))";

            if (strSearch != null && strSearch.trim().length() > 0) {
                String[] __fieldList = {"name_1", "code"};
                String[] __keyword = strSearch.trim().split(" ");
                for (int __field = 0; __field < __fieldList.length; __field++) {
                    if (__keyword.length > 0) {
                        if (__where.length() > 0) {
                            __where.append(" or ");
                        } else {
                            __where.append("  ");
                        }
                        __where.append("(");
                        for (int __loop = 0; __loop < __keyword.length; __loop++) {
                            if (__loop > 0) {
                                __where.append(" and ");
                            }
                            __where.append("upper(").append(__fieldList[__field])
                                    .append(") like '%").append(__keyword[__loop].toUpperCase()).append("%'");
                        }
                        __where.append(")");
                    }
                }
                _where += " and (" + __where.toString() + ") ";
            }

            _whereFinal = _where;

            if (strCategory != null && strCategory.trim().length() > 0) {
                _whereFinal += " and b.item_category='" + strCategory + "' ";
            }

            if ("1".equals(strPremium)) {
                _whereFinal += " and c.is_premium='1' ";
            }
            if ("1".equals(strFavorite)) {
                _whereFinal += " and arc.status='1' ";
            }

            // --- เงื่อนไขใหม่: ถ้า isstock = "1" ให้ดึงเฉพาะ stock > 0 ---
            if ("1".equals(strStock)) {
                _whereFinal += " and ((" + stockQtyExpr + ") > ROUND(coalesce(c.minimum_qty,0))) ";
            }

            if (strGroup != null && !strGroup.trim().isEmpty() && !"all".equalsIgnoreCase(strGroup)) {
                _whereFinal += " and b.group_main='" + strGroup + "' ";
            }

            if (strGroupSub != null && !strGroupSub.trim().isEmpty() && !"all".equalsIgnoreCase(strGroupSub)) {
                _whereFinal += " and b.group_sub='" + strGroupSub + "' ";
            }

            if (strGroupSub2 != null && !strGroupSub2.trim().isEmpty() && !"all".equalsIgnoreCase(strGroupSub2)) {
                _whereFinal += " and b.group_sub2='" + strGroupSub2 + "' ";
            }

            if (strBrand != null && !strBrand.trim().isEmpty() && !"all".equalsIgnoreCase(strBrand)) {
                _whereFinal += " and b.item_brand='" + strBrand + "' ";
            }

            if (strCategory2 != null && !strCategory2.trim().isEmpty() && !"all".equalsIgnoreCase(strCategory2)) {
                _whereFinal += " and b.item_category='" + strCategory2 + "' ";
            }

            if (strDesign != null && !strDesign.trim().isEmpty() && !"all".equalsIgnoreCase(strDesign)) {
                _whereFinal += " and b.item_design='" + strDesign + "' ";
            }

            if (strModel != null && !strModel.trim().isEmpty() && !"all".equalsIgnoreCase(strModel)) {
                _whereFinal += " and b.item_model='" + strModel + "' ";
            }

            String __strQUERY1
                    = "select b.code as item_code, b.name_1 as item_name, b.unit_cost,b.group_main, "
                    + " (case when (" + stockQtyExpr + ") <= ROUND(coalesce(c.minimum_qty,0)) then '1' else '0' end) as sold_out, "
                    + " coalesce(arc.status,0) as favorite_item "
                    + " from ic_inventory b "
                    + " left join ic_inventory_detail c on b.code=c.ic_code "
                    + " left join ar_item_by_customer arc on arc.ic_code = b.code and arc.ar_code='" + strCustCode + "' "
                    + " where b.supplier_code like '%" + strCustCode + "%' " + _whereFinal + " ";

            String __strQUERYCount
                    = "select count(b.code) as xcount "
                    + " from ic_inventory b "
                    + " left join ic_inventory_detail c on b.code=c.ic_code "
                    + " left join ar_item_by_customer arc on arc.ic_code = b.code and arc.ar_code='" + strCustCode + "' "
                    + " where b.supplier_code like '%" + strCustCode + "%' " + _whereFinal + " ";

            Statement __stmt1;
            ResultSet __rs1;
            // System.out.println(__strQUERY1);
            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1 + " offset " + strOffset + " limit " + strLimit);

            Statement __stmtTotal = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet _rsTotal = __stmtTotal.executeQuery(__strQUERYCount);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {
                JSONObject obj = new JSONObject();
                obj.put("item_code", __rs1.getString("item_code"));
                obj.put("item_name", __rs1.getString("item_name"));
                obj.put("group_main", __rs1.getString("group_main"));
                obj.put("sold_out", __rs1.getString("sold_out"));
                obj.put("favorite_item", __rs1.getString("favorite_item"));
                __jsonArr.put(obj);
            }
            __rs1.close();
            __stmt1.close();

            JSONObject objPage = new JSONObject();
            while (_rsTotal.next()) {
                objPage.put("total", _rsTotal.getInt("xcount"));
                objPage.put("perPage", Integer.parseInt(strLimit));
                objPage.put("page", Integer.parseInt(strOffset) / Integer.parseInt(strLimit));
                objPage.put("totalPage", Math.ceil(Double.parseDouble(_rsTotal.getString("xcount")) / Double.parseDouble(strLimit)));
            }
            _rsTotal.close();
            __stmtTotal.close();

            __conn.close();

            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);
            __objResponse.put("pagination", objPage);
        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getProductDetail")
    public Response getProductDetail(
            @QueryParam("cust_code") String strCustCode,
            @QueryParam("item_code") String strItemCode,
            @QueryParam("shelf_code") String strShelfCode,
            @QueryParam("sale_type") String strSaleType,
            @QueryParam("wh_code") String strWhCode
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));

            String strVatRate = "7";

            String __strQUERY1 = ""
                    + " select a.ic_code,b.name_1 as item_name,a.code as unit_code,c.maximum_qty,c.minimum_qty, coalesce((select price from ic_trans_detail where ic_trans_detail.item_code=b.code and ic_trans_detail.cust_code=b.supplier_code and ic_trans_detail.unit_code=a.code and trans_flag = 12 order by doc_date desc limit 1),0) as price,"
                    + " "
                    + " coalesce((select status from ar_item_by_customer where ic_code=b.code and ar_code='" + strCustCode + "' limit 1),0) as favorite_item,0 as price,\n"
                    + " c.start_sale_wh,c.start_sale_shelf,a.stand_value,a.divide_value,a.ratio\n"
                    + " from ic_unit_use a\n"
                    + " left join ic_inventory b on a.ic_code=b.code\n"
                    + " left join ic_inventory_detail c on a.ic_code=c.ic_code\n"
                    + " where a.ic_code in ('" + strItemCode + "')\n"
                    + " order by a.ic_code,ratio";
            System.out.println(__strQUERY1);
            Statement __stmt1;
            ResultSet __rs1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("barcode", "");
                obj.put("item_code", __rs1.getString("ic_code"));
                obj.put("item_name", __rs1.getString("item_name"));
                obj.put("unit_code", __rs1.getString("unit_code"));
                obj.put("maximum_qty", __rs1.getString("maximum_qty"));
                obj.put("minimum_qty", __rs1.getString("minimum_qty"));
                obj.put("balance_qty", "0");
                obj.put("sold_out", "0");
                obj.put("sum_sale", "0");
                obj.put("wh_code", __rs1.getString("start_sale_wh"));
                obj.put("shelf_code", __rs1.getString("start_sale_shelf"));
                obj.put("stand_value", __rs1.getString("stand_value"));
                obj.put("divide_value", __rs1.getString("divide_value"));
                obj.put("ratio", __rs1.getString("ratio"));
                obj.put("favorite_item", __rs1.getString("favorite_item"));
                obj.put("price", __rs1.getString("price"));
//                String year = "0";
//                if (!strShelfCode.equals("")) {
//                    year = strShelfCode;
//                }
//                String __strQUERYPrice = "SELECT "
//                        + "  ic_code,unit_code, "
//                        + "  CASE (EXTRACT(YEAR FROM NOW())::int - '" + year + "'::int) "
//                        + "    WHEN 0 THEN '0' "
//                        + "    WHEN 1 THEN price_1  "
//                        + "    WHEN 2 THEN price_2 "
//                        + "    WHEN 3 THEN price_3 "
//                        + "    WHEN 4 THEN price_4 "
//                        + "    ELSE NULL "
//                        + "  END AS price "
//                        + "FROM ic_inventory_price_formula  "
//                        + "WHERE ic_code = '" + __rs1.getString("ic_code") + "'  and sale_type = " + strSaleType + ";";
//
//                Statement __stmtPrice = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
//                ResultSet __rsPrice = __stmtPrice.executeQuery(__strQUERYPrice);
//                System.out.println("__strQUERYPrice " + __strQUERYPrice);
//                while (__rsPrice.next()) {
//                    obj.put("price", __rsPrice.getString("price"));
//                }
//
//                __rsPrice.close();
//                __stmtPrice.close();
//
//                if (obj.get("price").toString().equals("0")) {
//                    JSONObject prices = getProductPriceLocal(__rs1.getString("ic_code"), __rs1.getString("unit_code"), "1", strCustCode, strSaleType);
//
//                    JSONArray pricesArr = prices.optJSONArray("data");
//                    if (pricesArr != null && pricesArr.length() > 0) {
//                        JSONObject priceObj = pricesArr.optJSONObject(0);
//                        obj.put("price", priceObj != null ? priceObj.optString("price", "0") : "0");
//                    } else {
//                        obj.put("price", "0");
//                    }
//
//                }

                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getProductStock")
    public Response getProductStock(
            @QueryParam("item_code") String strItemCode,
            @QueryParam("unit_code") String strUnitCode,
            @QueryParam("wh_code") String strWarehouse,
            @QueryParam("sale_type") String strSaleType,
            @QueryParam("cust_code") String strCustCode
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));

            String strVatRate = "7";

            String __strQUERY1 = " select temp2.warehouse\n"
                    + ", ic_warehouse.name_1 as warehouse_name\n"
                    + ", temp2.location\n"
                    + ", ic_shelf.name_1 as location_name\n"
                    + ", temp2.ic_code\n"
                    + ", temp2.unit_code\n"
                    + ", ic_inventory.name_1 as ic_name\n"
                    + ", ic_inventory.unit_standard as ic_unit_code\n"
                    + ", temp2.balance_qty\n"
                    + " "
                    + " from ( "
                    + " select ic_code,unit_code, warehouse,location, balance_qty "
                    + " from ( "
                    + "  select ic_trans_detail.item_code as ic_code,ic_trans_detail.unit_code as unit_code,ic_trans_detail.wh_code as warehouse "
                    + "  ,ic_trans_detail.shelf_code as location\n"
                    + "  ,coalesce(sum(ic_trans_detail.calc_flag*(case when (ic_trans_detail.trans_flag in (70,54,60,58,310,12) or (ic_trans_detail.trans_flag=66 and ic_trans_detail.qty>0) or (ic_trans_detail.trans_flag=14 and ic_trans_detail.inquiry_type=0) or (ic_trans_detail.trans_flag=48 and ic_trans_detail.inquiry_type < 2)) or (ic_trans_detail.trans_flag in (56,68,72,44) or (ic_trans_detail.trans_flag=66 and ic_trans_detail.qty<0) or (ic_trans_detail.trans_flag=46 and ic_trans_detail.inquiry_type in (0,2))  or (ic_trans_detail.trans_flag=16 and ic_trans_detail.inquiry_type in (0,2)) or (ic_trans_detail.trans_flag=311 and ic_trans_detail.inquiry_type=0)) then ic_trans_detail.qty*(ic_trans_detail.stand_value / ic_trans_detail.divide_value) else 0 end)),0) as balance_qty\n"
                    + " from ic_trans_detail\n"
                    + " join ic_inventory on  ic_trans_detail.item_code = ic_inventory.code\n"
                    + " where ic_trans_detail.last_status=0 and ic_trans_detail.item_type<>5 and ic_inventory.item_type<>1 and ic_trans_detail.doc_date_calc<= now()\n"
                    + "  and ( ic_trans_detail.item_code = '" + strItemCode + "'  and ic_trans_detail.unit_code ='" + strUnitCode + "'  )  "
                    + " group by ic_trans_detail.item_code,ic_trans_detail.unit_code,ic_trans_detail.wh_code,ic_trans_detail.wh_code,ic_trans_detail.shelf_code "
                    + " order by ic_trans_detail.wh_code desc,ic_trans_detail.shelf_code desc,ic_trans_detail.item_code,ic_trans_detail.unit_code,ic_trans_detail.wh_code,ic_trans_detail.shelf_code "
                    + " "
                    + " ) as temp1 "
                    + " ) as temp2  "
                    + " join  ic_inventory on ic_inventory.code=temp2.ic_code "
                    + " join ic_warehouse on ic_warehouse.code = temp2.warehouse "
                    + " join ic_shelf on ic_shelf.code = temp2.location and ic_shelf.whcode = temp2.warehouse "
                    + " where temp2.balance_qty > 0 ";
            System.out.println(__strQUERY1);
            Statement __stmt1;
            ResultSet __rs1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("warehouse", __rs1.getString("warehouse"));
                obj.put("warehouse_name", __rs1.getString("warehouse_name"));
                obj.put("location", __rs1.getString("location"));
                obj.put("location_name", __rs1.getString("location_name"));
                obj.put("item_code", __rs1.getString("ic_code"));
                obj.put("item_name", __rs1.getString("ic_name"));
                obj.put("unit_code", __rs1.getString("unit_code"));
                obj.put("price", "0");
                obj.put("unit_standard_code", __rs1.getString("ic_unit_code"));
                obj.put("balance_qty", __rs1.getString("balance_qty"));
                String location = __rs1.getString("location");
                String year = "0";

                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getProductStockByLocation")
    public Response getProductStockByLocation(
            @QueryParam("item_code") String strItemCode,
            @QueryParam("unit_code") String strUnitCode,
            @QueryParam("wh_code") String strWarehouse,
            @QueryParam("shelf_code") String strShelfCode
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));

            String strVatRate = "7";

            String __strQUERY1 = "select temp2.warehouse\n"
                    + ", temp2.location\n"
                    + ", temp2.ic_code\n"
                    + ", temp2.unit_code\n"
                    + ", ic_inventory.name_1 as ic_name\n"
                    + ", temp2.balance_qty\n"
                    + "  from (  select ic_code,unit_code, warehouse,location, balance_qty  from (   select ic_trans_detail.item_code as ic_code,ic_trans_detail.unit_code as unit_code,ic_trans_detail.wh_code as warehouse   ,ic_trans_detail.shelf_code as location\n"
                    + "  ,coalesce(sum(ic_trans_detail.calc_flag*(case when (ic_trans_detail.trans_flag in (70,54,60,58,310,12) or (ic_trans_detail.trans_flag=66 and ic_trans_detail.qty>0) or (ic_trans_detail.trans_flag=14 and ic_trans_detail.inquiry_type=0) or (ic_trans_detail.trans_flag=48 and ic_trans_detail.inquiry_type < 2)) or (ic_trans_detail.trans_flag in (56,68,72,44) or (ic_trans_detail.trans_flag=66 and ic_trans_detail.qty<0) or (ic_trans_detail.trans_flag=46 and ic_trans_detail.inquiry_type in (0,2))  or (ic_trans_detail.trans_flag=16 and ic_trans_detail.inquiry_type in (0,2)) or (ic_trans_detail.trans_flag=311 and ic_trans_detail.inquiry_type=0)) then ic_trans_detail.qty*(ic_trans_detail.stand_value / ic_trans_detail.divide_value) else 0 end)),0) as balance_qty\n"
                    + " from ic_trans_detail\n"
                    + " join ic_inventory on  ic_trans_detail.item_code = ic_inventory.code\n"
                    + " where ic_trans_detail.last_status=0 and ic_trans_detail.item_type<>5 and ic_inventory.item_type<>1 and ic_trans_detail.doc_date_calc<= now()\n"
                    + "  and ( ic_trans_detail.item_code = '" + strItemCode + "'  and ic_trans_detail.unit_code ='" + strUnitCode + "' and ic_trans_detail.wh_code = '" + strWarehouse + "' and ic_trans_detail.shelf_code = '" + strShelfCode + "')   \n"
                    + "  group by ic_trans_detail.item_code,ic_trans_detail.unit_code,ic_trans_detail.wh_code,ic_trans_detail.shelf_code  \n"
                    + "  order by ic_trans_detail.wh_code desc,ic_trans_detail.shelf_code desc,ic_trans_detail.item_code,ic_trans_detail.unit_code,ic_trans_detail.wh_code,ic_trans_detail.shelf_code   ) as temp1  ) as temp2   \n"
                    + "  join  ic_inventory on ic_inventory.code=temp2.ic_code  join ic_warehouse on ic_warehouse.code = temp2.warehouse  \n"
                    + "  join ic_shelf on ic_shelf.code = temp2.location and ic_shelf.whcode = temp2.warehouse  where temp2.balance_qty > 0  limit 1";
            System.out.println(__strQUERY1);
            Statement __stmt1;
            ResultSet __rs1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            JSONObject obj = new JSONObject();
            obj.put("warehouse", strWarehouse);
            obj.put("location", strShelfCode);
            obj.put("item_code", strItemCode);
            obj.put("unit_code", strUnitCode);
            obj.put("balance_qty", "0");
            while (__rs1.next()) {

                obj.put("warehouse", __rs1.getString("warehouse"));
                obj.put("location", __rs1.getString("location"));
                obj.put("item_code", __rs1.getString("ic_code"));
                obj.put("unit_code", __rs1.getString("unit_code"));
                obj.put("balance_qty", __rs1.getString("balance_qty"));
                __objResponse.put("success", true);
            }
            __rs1.close();
            __stmt1.close();
            __conn.close();

            __objResponse.put("data", obj);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getProductStockPriceByLocation")
    public Response getProductStockPriceByLocation(
            @QueryParam("item_code") String strItemCode,
            @QueryParam("unit_code") String strUnitCode,
            @QueryParam("wh_code") String strWarehouse,
            @QueryParam("shelf_code") String strShelfCode,
            @QueryParam("sale_type") String strSaleType,
            @QueryParam("cust_code") String strCustCode
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));

            String strVatRate = "7";

            String __strQUERY1 = "select temp2.warehouse\n"
                    + ", ic_warehouse.name_1 as warehouse_name\n"
                    + ", temp2.location\n"
                    + ", ic_shelf.name_1 as location_name\n"
                    + ", temp2.ic_code\n"
                    + ", temp2.unit_code\n"
                    + ", ic_inventory.name_1 as ic_name\n"
                    + ", temp2.balance_qty\n"
                    + ", icu.stand_value\n"
                    + ", icu.divide_value\n"
                    + ", icu.ratio \n"
                    + "  from (  select ic_code,unit_code, warehouse,location, balance_qty  from (   select ic_trans_detail.item_code as ic_code,ic_trans_detail.unit_code as unit_code,ic_trans_detail.wh_code as warehouse   ,ic_trans_detail.shelf_code as location\n"
                    + "  ,coalesce(sum(ic_trans_detail.calc_flag*(case when (ic_trans_detail.trans_flag in (70,54,60,58,310,12) or (ic_trans_detail.trans_flag=66 and ic_trans_detail.qty>0) or (ic_trans_detail.trans_flag=14 and ic_trans_detail.inquiry_type=0) or (ic_trans_detail.trans_flag=48 and ic_trans_detail.inquiry_type < 2)) or (ic_trans_detail.trans_flag in (56,68,72,44) or (ic_trans_detail.trans_flag=66 and ic_trans_detail.qty<0) or (ic_trans_detail.trans_flag=46 and ic_trans_detail.inquiry_type in (0,2))  or (ic_trans_detail.trans_flag=16 and ic_trans_detail.inquiry_type in (0,2)) or (ic_trans_detail.trans_flag=311 and ic_trans_detail.inquiry_type=0)) then ic_trans_detail.qty*(ic_trans_detail.stand_value / ic_trans_detail.divide_value) else 0 end)),0) as balance_qty\n"
                    + " from ic_trans_detail\n"
                    + " join ic_inventory on  ic_trans_detail.item_code = ic_inventory.code\n"
                    + "\n"
                    + " where ic_trans_detail.last_status=0 and ic_trans_detail.item_type<>5 and ic_inventory.item_type<>1 and ic_trans_detail.doc_date_calc<= now()\n"
                    + "  and ( ic_trans_detail.item_code = '" + strItemCode + "'  and ic_trans_detail.unit_code ='" + strUnitCode + "' and ic_trans_detail.wh_code = '" + strWarehouse + "' and ic_trans_detail.shelf_code = '" + strShelfCode + "')   \n"
                    + "  group by ic_trans_detail.item_code,ic_trans_detail.unit_code,ic_trans_detail.wh_code,ic_trans_detail.wh_code,ic_trans_detail.shelf_code  \n"
                    + "  order by ic_trans_detail.wh_code desc,ic_trans_detail.shelf_code desc,ic_trans_detail.item_code,ic_trans_detail.unit_code,ic_trans_detail.wh_code,ic_trans_detail.shelf_code   ) as temp1  ) as temp2   \n"
                    + "  join  ic_inventory on ic_inventory.code=temp2.ic_code \n"
                    + " left join ic_unit_use icu on icu.ic_code =  temp2.ic_code and icu.code = temp2.unit_code \n"
                    + "  join ic_warehouse on ic_warehouse.code = temp2.warehouse  join ic_shelf on ic_shelf.code = temp2.location and ic_shelf.whcode = temp2.warehouse  where temp2.balance_qty > 0 limit 1 ";
            System.out.println(__strQUERY1);
            Statement __stmt1;
            ResultSet __rs1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            JSONObject obj = new JSONObject();
            obj.put("warehouse", strWarehouse);
            obj.put("location", strShelfCode);
            obj.put("item_code", strItemCode);
            obj.put("unit_code", strUnitCode);
            obj.put("balance_qty", "0");
            obj.put("price", "0");
            while (__rs1.next()) {

                obj.put("warehouse", __rs1.getString("warehouse"));
                obj.put("location", __rs1.getString("location"));
                obj.put("item_code", __rs1.getString("ic_code"));
                obj.put("unit_code", __rs1.getString("unit_code"));
                obj.put("balance_qty", __rs1.getString("balance_qty"));

                String location = __rs1.getString("location");
                String year = "0";
                if (location != null && location.trim().matches("\\d{4}")) {
                    year = location.substring(0, 2);
                    String __strQUERYPrice = "SELECT "
                            + "  ic_code,unit_code, "
                            + "  CASE (RIGHT(EXTRACT(YEAR FROM NOW())::int::text, 2)::int - '" + year + "'::int) "
                            + "    WHEN 0 THEN '0' "
                            + "    WHEN 1 THEN coalesce(price_1,'0')  "
                            + "    WHEN 2 THEN coalesce(price_2,'0') "
                            + "    WHEN 3 THEN coalesce(price_3,'0') "
                            + "    WHEN 4 THEN coalesce(price_4,'0') "
                            + "    ELSE '0' "
                            + "  END AS price "
                            + "FROM ic_inventory_price_formula  "
                            + "WHERE ic_code = '" + __rs1.getString("ic_code") + "'  and sale_type in (0," + strSaleType + ");";

                    Statement __stmtPrice = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    ResultSet __rsPrice = __stmtPrice.executeQuery(__strQUERYPrice);
                    System.out.println("__strQUERYPrice " + __strQUERYPrice);
                    while (__rsPrice.next()) {
                        obj.put("price", __rsPrice.getString("price"));
                    }

                    __rsPrice.close();
                    __stmtPrice.close();
                }

                if (obj.get("price").toString().equals("0")) {

                    JSONObject prices = getProductPriceLocal(__rs1.getString("ic_code"), __rs1.getString("unit_code"), "1", strCustCode, strSaleType);

                    JSONArray pricesArr = prices.optJSONArray("data");
                    if (pricesArr != null && pricesArr.length() > 0) {
                        JSONObject priceObj = pricesArr.optJSONObject(0);
                        obj.put("price", priceObj != null ? priceObj.optString("price", "0") : "0");
                    } else {
                        obj.put("price", "0");
                    }

                }

                __objResponse.put("success", true);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();

            __objResponse.put("data", obj);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getProductBalancePrice")
    public Response getProductBalancePrice(
            @QueryParam("item_code") String strItemCode,
            @QueryParam("unit_code") String strUnit,
            @QueryParam("cust_code") String strCust,
            @QueryParam("shelf_code") String strShelfCode,
            @QueryParam("wh_code") String strWhCode,
            @QueryParam("sale_type") String strSaleType
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));

            String strVatRate = "7";

            String __strQUERY1 = "select a.ic_code,a.barcode,b.name_1 as item_name,a.unit_code,\n"
                    + "(((coalesce((select max(balance_qty) from sml_ic_function_stock_balance_warehouse_location('NOW()',a.ic_code, '" + strWhCode + "', '" + strShelfCode + "') where ic_unit_code = a.unit_code ),0)/((select unit_standard_stand_value from ic_inventory where code=a.ic_code)/(select unit_standard_divide_value from ic_inventory where code=a.ic_code))))-(select accrued_out_qty from ic_inventory where code=a.ic_code)) as sum_balance_qty\n"
                    + ",coalesce((select max(balance_qty) from sml_ic_function_stock_balance_warehouse_location('NOW()',a.ic_code, '" + strWhCode + "', '" + strShelfCode + "') where ic_unit_code = a.unit_code ),0) as balance_qty,\n"
                    + "(Case when (coalesce((select max(balance_qty) from sml_ic_function_stock_balance_warehouse_location('NOW()',a.ic_code,'" + strWhCode + "', '" + strShelfCode + "') where ic_unit_code = a.unit_code limit 1),0)/((select unit_standard_stand_value from ic_inventory where code=a.ic_code)/(select unit_standard_divide_value from ic_inventory where code=a.ic_code))) <= ROUND((coalesce(c.maximum_qty,0)*5)/100) then '1' else '0' end)as sold_out\n"
                    + ",coalesce(((select sum(qty) from ic_trans_detail where a.ic_code=item_code and a.unit_code=unit_code and doc_date between '2025-01-01' and 'NOW()')\n"
                    + "*(select stand_value from ic_trans_detail where a.ic_code=item_code and a.unit_code=unit_code limit 1)),0) as sum_sale,\n"
                    + "coalesce((select status from ar_item_by_customer where ic_code=a.ic_code and ar_code='" + strCust + "' limit 1),0) as favorite_item,"
                    + "c.start_sale_wh,c.start_sale_shelf, \n"
                    + "0 as price,icu.stand_value,icu.divide_value,icu.ratio \n"
                    + "from ic_inventory_barcode a left join ic_inventory b on a.ic_code=b.code\n"
                    + "left join ic_inventory_detail c on a.ic_code=c.ic_code left join ic_unit_use icu on icu.code = a.unit_code and icu.ic_code = a.ic_code where a.ic_code = '" + strItemCode + "' and a.unit_code = '" + strUnit + "' ";

            Statement __stmt1;
            ResultSet __rs1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("barcode", __rs1.getString("barcode"));
                obj.put("item_code", __rs1.getString("ic_code"));
                obj.put("item_name", __rs1.getString("item_name"));
                obj.put("unit_code", __rs1.getString("unit_code"));
                obj.put("balance_qty", __rs1.getString("sum_balance_qty"));
                obj.put("sold_out", __rs1.getString("sold_out"));
                obj.put("sum_sale", __rs1.getString("sum_sale"));
                obj.put("wh_code", __rs1.getString("start_sale_wh"));
                obj.put("shelf_code", __rs1.getString("start_sale_shelf"));
                obj.put("stand_value", __rs1.getString("stand_value"));
                obj.put("divide_value", __rs1.getString("divide_value"));
                obj.put("ratio", __rs1.getString("ratio"));
                obj.put("favorite_item", __rs1.getString("favorite_item"));
                obj.put("price", 0);
                String year = "0";
                if (!strShelfCode.equals("")) {
                    year = strShelfCode;
                }
                String __strQUERYPrice = "SELECT "
                        + "  ic_code,unit_code, "
                        + "  CASE (EXTRACT(YEAR FROM NOW())::int - '" + year + "'::int) "
                        + "    WHEN 0 THEN '0' "
                        + "    WHEN 1 THEN price_1  "
                        + "    WHEN 2 THEN price_2 "
                        + "    WHEN 3 THEN price_3 "
                        + "    WHEN 4 THEN price_4 "
                        + "    ELSE NULL "
                        + "  END AS price "
                        + "FROM ic_inventory_price_formula  "
                        + "WHERE ic_code = '" + __rs1.getString("ic_code") + "' and unit_code = '" + __rs1.getString("unit_code") + "' and sale_type in (0," + strSaleType + ");";
                Statement __stmtPrice = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet __rsPrice = __stmtPrice.executeQuery(__strQUERYPrice);

                while (__rsPrice.next()) {
                    obj.put("price", __rs1.getString("price"));
                }

                __rsPrice.close();
                __stmtPrice.close();

                if (obj.get("price").toString().equals("0")) {
                    JSONObject prices = getProductPriceLocal(__rs1.getString("ic_code"), __rs1.getString("unit_code"), "1", strCust, strSaleType);

                    JSONArray pricesArr = prices.optJSONArray("data");
                    if (pricesArr != null && pricesArr.length() > 0) {
                        JSONObject priceObj = pricesArr.optJSONObject(0);
                        obj.put("price", priceObj != null ? priceObj.optString("price", "0") : "0");
                    } else {
                        obj.put("price", "0");
                    }

                }

                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getCategoryList")
    public Response getCategoryList() {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));

            String __strQUERY1 = "select code,name_1 from ic_category ";

            Statement __stmt1;
            ResultSet __rs1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("code", __rs1.getString("code"));
                obj.put("name", __rs1.getString("name_1"));
                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getWarehouseList")
    public Response getWarehouseList() {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));

            String __strQUERY1 = "select code,name_1 from ic_warehouse ";

            Statement __stmt1;
            ResultSet __rs1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("code", __rs1.getString("code"));
                obj.put("name", __rs1.getString("name_1"));
                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/deleteItem")
    public Response deleteItem(@QueryParam("guid_code") String strGuidCode, @QueryParam("cust_code") String strCustCode) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";

            String __strQUERY1 = "delete from ps_cart_order_temp where guid_code = '" + strGuidCode + "' and cust_code = '" + strCustCode + "' ";

            System.out.println("" + __strQUERY1);
            Statement __stmtz;
            __stmtz = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __stmtz.executeUpdate(__strQUERY1);
            __stmtz.close();
            __conn.close();
            __objResponse.put("success", true);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/deleteAllItems")
    public Response deleteAllItems(@QueryParam("cust_code") String strCustCode) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";

            String __strQUERY1 = "delete from ps_cart_order_temp where cust_code = '" + strCustCode + "'";

            System.out.println("" + __strQUERY1);
            Statement __stmtz;
            __stmtz = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __stmtz.executeUpdate(__strQUERY1);
            __stmtz.close();
            __conn.close();
            __objResponse.put("success", true);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getCustomerList")
    public Response getCustomerList(@QueryParam("search") String strCustCode) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";
            if (!strCustCode.equals("")) {
                where += " and ( code like '%" + strCustCode + "%'  or name_1 like '%" + strCustCode + "%') ";
            }
            String __strQUERY1 = "SELECT code as user_code,name_1 as user_name,address,telephone,coalesce((select tax_id from ar_customer_detail where ar_code = code),'') as tax_id FROM ar_customer where 1=1 " + where + " limit 50";

            Statement __stmt1;
            ResultSet __rs1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("code", __rs1.getString("user_code"));
                obj.put("name", __rs1.getString("user_name"));
                obj.put("address", __rs1.getString("address"));
                obj.put("telephone", __rs1.getString("telephone"));
                obj.put("tax_id", __rs1.getString("tax_id"));
                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getSupplierList")
    public Response getSupplierList(@QueryParam("search") String strCustCode) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";
            if (!strCustCode.equals("")) {
                where += " and ( code like '%" + strCustCode + "%'  or name_1 like '%" + strCustCode + "%') ";
            }
            String __strQUERY1 = "SELECT code as user_code,name_1 as user_name FROM ap_supplier where 1=1 " + where + " limit 50";

            Statement __stmt1;
            ResultSet __rs1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("code", __rs1.getString("user_code"));
                obj.put("name", __rs1.getString("user_name"));

                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getEmployeeList")
    public Response getEmployeeList(@QueryParam("search") String strCustCode) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";
            if (!strCustCode.equals("")) {
                where += " and ( code like '%" + strCustCode + "%'  or name_1 like '%" + strCustCode + "%') ";
            }
            String __strQUERY1 = "SELECT code,name_1 as name FROM erp_user  where name_2='' " + where + " limit 50";

            Statement __stmt1;
            ResultSet __rs1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("code", __rs1.getString("code"));
                obj.put("name", __rs1.getString("name"));

                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getCustomerCRM")
    public Response getCustomerCRM(@QueryParam("search") String strCustCode,
            @QueryParam("limit") @DefaultValue("50") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));

            String where = "";
            if (strCustCode != null && !strCustCode.trim().equals("")) {
                where += " and ( code like '%" + strCustCode + "%'  or name_1 like '%" + strCustCode + "%') ";
            }

            // Step 1: Get total count
            String countQuery = "SELECT COUNT(*) as total FROM ar_customer WHERE 1=1 " + where;
            Statement countStmt = __conn.createStatement();
            ResultSet countRs = countStmt.executeQuery(countQuery);

            int total = 0;
            if (countRs.next()) {
                total = countRs.getInt("total");
            }
            countRs.close();
            countStmt.close();

            // Step 2: Get paginated data
            String __strQUERY1 = "SELECT code as user_code,name_1 as user_name,coalesce(address,'') as address,coalesce(telephone,'') as telephone,coalesce(website,'') as website,"
                    + "coalesce((select logistic_area from ar_customer_detail where ar_code = code),'') as logistic_area, "
                    + "coalesce((select group_main from ar_customer_detail where ar_code = code),'') as group_main "
                    + "FROM ar_customer where 1=1 " + where
                    + " limit " + limit + " offset " + offset;

            Statement __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {
                JSONObject obj = new JSONObject();
                obj.put("code", __rs1.getString("user_code"));
                obj.put("name", __rs1.getString("user_name"));
                obj.put("address", __rs1.getString("address"));
                obj.put("telephone", __rs1.getString("telephone"));
                obj.put("logistic_area", __rs1.getString("logistic_area"));
                obj.put("gps", __rs1.getString("website"));
                obj.put("group_main", __rs1.getString("group_main"));
                __jsonArr.put(obj);
            }

            __rs1.close();
            __stmt1.close();
            __conn.close();

            int currentPage = (offset / limit) + 1;
            int totalPages = (int) Math.ceil((double) total / limit);

            JSONObject pagination = new JSONObject();
            pagination.put("total", total);
            pagination.put("limit", limit);
            pagination.put("offset", offset);
            pagination.put("current_page", currentPage);
            pagination.put("total_page", totalPages);

            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);
            __objResponse.put("pagination", pagination);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getEmployeeCRM")
    public Response getEmployeeCRM(@QueryParam("search") String strCustCode,
            @QueryParam("limit") @DefaultValue("50") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";
            if (strCustCode != null && !strCustCode.trim().equals("")) {
                where += " and ( code like '%" + strCustCode + "%'  or name_1 like '%" + strCustCode + "%') ";
            }

            String __strQUERY1 = "SELECT code,name_1 as name FROM erp_user "
                    + "where name_2='o' " + where
                    + " limit " + limit + " offset " + offset;

            Statement __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {
                JSONObject obj = new JSONObject();
                obj.put("code", __rs1.getString("code"));
                obj.put("name", __rs1.getString("name"));
                __jsonArr.put(obj);
            }

            __rs1.close();
            __stmt1.close();
            __conn.close();

            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getCompanyProfile")
    public Response getCompanyProfile() {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";

            String __strQUERY1 = "select company_name_1,address_1,telephone_number from erp_company_profile limit 1";

            Statement __stmt1;
            ResultSet __rs1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("company_name", __rs1.getString("company_name_1"));
                obj.put("address", __rs1.getString("address_1"));
                obj.put("telephone_number", __rs1.getString("telephone_number"));
                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getImageList")
    public Response getImageList(@QueryParam("item_code") String strItemCode) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1_images";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        try {
            _routine __routine = new _routine();
            Connection __conn = __routine._connect(strDatabaseName, _global.FILE_CONFIG(strProvider));
            String where = "";

            String __strQUERY1 = "select image_id,guid_code from images  where image_id = '" + strItemCode + "' order by image_order asc";

            Statement __stmt1;
            ResultSet __rs1;

            __stmt1 = __conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            __rs1 = __stmt1.executeQuery(__strQUERY1);

            JSONArray __jsonArr = new JSONArray();
            while (__rs1.next()) {

                JSONObject obj = new JSONObject();
                obj.put("image_id", __rs1.getString("image_id"));
                obj.put("guid_code", __rs1.getString("guid_code"));
                __jsonArr.put(obj);

            }
            __rs1.close();
            __stmt1.close();
            __conn.close();
            __objResponse.put("success", true);
            __objResponse.put("data", __jsonArr);

        } catch (Exception ex) {
            return Response.status(400).entity("{ERROR: " + ex.getMessage() + "}").build();
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    @Path("/imagesguid")
    @GET
    @Produces("image/png")
    public Response imagesguid(
            @QueryParam("guid_code") String strItemCode) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1_images";
        String query = "select image_file from images  where guid_code = '" + strItemCode + "'";

        byte[] __value = new byte[1024];
        _routine __routine = new _routine();
        Connection __conn = __routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));
        try {
            PreparedStatement __stmtHeader;
            __stmtHeader = __conn.prepareStatement(query);
            ResultSet __rsHead1 = __stmtHeader.executeQuery();
            ResultSetMetaData __rsmd = __rsHead1.getMetaData();
            int __colCount = __rsmd.getColumnCount();
            while (__rsHead1.next()) {
                for (int __i = 1; __i <= __colCount; __i++) {
                    // String columnName = rsmd.getColumnName(i);
                    __value = __rsHead1.getBytes(__i);
                }
            }
            __conn.close();
        } catch (Exception __ex) {

        }

        OutputStream out = null;

        return Response.status(200).entity(new ByteArrayInputStream(__value)).build();
    }

    @GET
    @Path("/images")
    @Produces("image/png")
    public Response _getImage(
            @QueryParam("item_code") String itemCode,
            @Context Request request
    ) {
        String strProvider = "DEMO";
        String strDatabaseName = "demo1_images";

        if (itemCode == null || itemCode.trim().isEmpty()) {
            return Response.status(400)
                    .type(MediaType.TEXT_PLAIN)
                    .entity("ERROR: item_code is required")
                    .build();
        }

        byte[] imageBytes = null;

        // กัน SQL injection
        String sql = "select image_file from images where image_id = ? and image_order = 0 limit 1";

        _routine routine = new _routine();

        try (Connection conn = routine._connect(strDatabaseName.toLowerCase(), _global.FILE_CONFIG(strProvider));
                PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, itemCode);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    imageBytes = rs.getBytes(1);
                }
            }

        } catch (Exception ex) {
            return Response.status(500)
                    .type(MediaType.TEXT_PLAIN)
                    .entity("ERROR: " + ex.getMessage())
                    .build();
        }

        if (imageBytes == null || imageBytes.length == 0) {
            return Response.status(Response.Status.NOT_FOUND)
                    .type(MediaType.TEXT_PLAIN)
                    .entity("ERROR: image not found")
                    .build();
        }

        // Cache-Control
        CacheControl cc = new CacheControl();
        cc.setPrivate(false);
        cc.setNoStore(false);
        cc.setNoCache(false);
        cc.setMaxAge(604800);

        // ETag (ใช้ hash ของ bytes เพื่อให้ client revalidate แล้วได้ 304)
        EntityTag etag = new EntityTag(md5Hex(imageBytes));

        // ถ้า client ส่ง If-None-Match ที่ตรงกับ etag -> return 304
        Response.ResponseBuilder precond = request.evaluatePreconditions(etag);
        if (precond != null) {
            return precond
                    .cacheControl(cc)
                    .tag(etag)
                    .build();
        }

        // 200 OK + ส่งรูป
        return Response.ok(new ByteArrayInputStream(imageBytes))
                .type("image/png")
                .cacheControl(cc)
                .tag(etag)
                .header("Content-Length", String.valueOf(imageBytes.length))
                .build();
    }

    private String md5Hex(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(data);
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }
            return sb.toString();
        } catch (Exception e) {
            // fallback กัน error
            return String.valueOf(data.length);
        }
    }

    public JSONObject getProductPriceLocal(
            String strICCode,
            String strUnitCode,
            String strQTY,
            String strCustomerCode,
            String strSaleType) {

        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        String strVatType = "ภาษีรวมใน";

        String strVatRate = "7";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        String strBarcode = "";

        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);

        String strCondition_Today = " ('" + ft.format(new Date()) + "' >= from_date  AND '" + ft.format(new Date()) + "' <= to_date) ";
        String strCondition_QTY = " ('" + (strQTY.equals("0") ? "1" : strQTY) + "' >= from_qty AND '" + (strQTY.equals("0") ? "1" : strQTY) + "' <= to_qty) ";

        String strCondition_Today_barcodeDiscount = " AND ('" + ft.format(new Date()) + "' >= date_begin  AND '" + ft.format(new Date()) + "' <= date_end) ";
        String strCondition_QTY_barcodeDiscount = " AND ('" + (strQTY.equals("0") ? "1" : strQTY) + "' >= qty_begin AND '" + (strQTY.equals("0") ? "1" : strQTY) + "' <= qty_end) ";

        String strField_Price = strVatType.equals("ภาษีแยกนอก") || strVatType.equals("Tax Excluded") ? "sale_price1" : "sale_price2";
        String strSaleFilterType = Integer.parseInt(strSaleType) == 0 || Integer.parseInt(strSaleType) == 2 ? "2" : "1";
        String strCondition_SaleType = " AND sale_type IN (0," + strSaleFilterType + ") ";
        String strCondition_TransportType = "";

        // 0 = ราคาตามลูกค้า , 1 = ราคาตามกลุ่มลูกค้า
        // 2 = ราคาขายทั่วไป , 3 = ราคามาตราฐาน
        // 4 = ราคาตามสูตร , 5 = ราคาตามสูตร
        // 6 = ราคาตามบาร์โค้ด
        myglobal __myGlobal = new myglobal();
        List<String> __QueryList = new ArrayList<>();
        String strSubWhere = " AND ("
                + " (cust_group_2   = (SELECT group_sub_1 FROM ar_customer_detail  WHERE  ar_customer_detail.ar_code = '" + strCustomerCode + "')) "
                + " OR (cust_group_2   = (SELECT group_sub_2 FROM ar_customer_detail  WHERE  ar_customer_detail.ar_code = '" + strCustomerCode + "')) "
                + " OR (cust_group_2   = (SELECT group_sub_3 FROM ar_customer_detail  WHERE  ar_customer_detail.ar_code = '" + strCustomerCode + "')) "
                + " OR (cust_group_2   = (SELECT group_sub_4 FROM ar_customer_detail  WHERE  ar_customer_detail.ar_code = '" + strCustomerCode + "')) "
                + " OR (COALESCE(cust_group_2, '')= '')"
                + ") ";

        // 0 = ราคาตามลูกค้า
        __QueryList.add("SELECT roworder ,ic_inventory_price.sale_price1, ic_inventory_price.sale_price2,ic_inventory_price.price_mode "
                + " FROM ic_inventory_price "
                + " WHERE ic_inventory_price.ic_code='" + strICCode + "' "
                + " AND ic_inventory_price.unit_code='" + strUnitCode + "' "
                + " AND ic_inventory_price.cust_code='" + strCustomerCode + "' "
                + " AND ic_inventory_price.price_type=3 "
                + " AND " + strCondition_Today + " "
                + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " "
                + " ORDER BY ic_inventory_price.price_mode DESC, ic_inventory_price.sale_type DESC, ic_inventory_price.transport_type DESC");

        // 1 = ราคาตามกลุ่มลูกค้า
        __QueryList.add("SELECT roworder ,ic_inventory_price.sale_price1, ic_inventory_price.sale_price2,ic_inventory_price.price_mode "
                + " FROM ic_inventory_price "
                + " WHERE ic_inventory_price.ic_code='" + strICCode + "' "
                + " AND ic_inventory_price.unit_code='" + strUnitCode + "' "
                + " AND ic_inventory_price.cust_group_1=(SELECT ar_customer_detail.group_main FROM ar_customer_detail WHERE ar_customer_detail.ar_code='" + strCustomerCode + "') "
                + " " + strSubWhere + " "
                + " AND ic_inventory_price.price_type=2 "
                + " AND " + strCondition_Today + " "
                + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " "
                + " ORDER BY ic_inventory_price.price_mode DESC, ic_inventory_price.sale_type DESC, ic_inventory_price.transport_type DESC, ic_inventory_price.cust_group_2");

        // 2 = ราคาขายทั่วไป
        __QueryList.add("SELECT roworder ,ic_inventory_price.sale_price1, ic_inventory_price.sale_price2,ic_inventory_price.price_mode "
                + " FROM ic_inventory_price "
                + " WHERE ic_inventory_price.ic_code='" + strICCode + "' "
                + " AND ic_inventory_price.unit_code='" + strUnitCode + "' "
                + " AND ic_inventory_price.price_type=1 "
                + " AND " + strCondition_Today + " "
                + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " "
                + " AND ic_inventory_price.price_mode=1 " // price_mode = 1
                + " ORDER BY ic_inventory_price.price_mode DESC, ic_inventory_price.sale_type DESC, ic_inventory_price.transport_type DESC");

        // 3 = ราคามาตราฐาน
        __QueryList.add("SELECT roworder ,ic_inventory_price.sale_price1, ic_inventory_price.sale_price2,ic_inventory_price.price_mode "
                + " FROM ic_inventory_price "
                + " WHERE ic_inventory_price.ic_code='" + strICCode + "' "
                + " AND ic_inventory_price.unit_code='" + strUnitCode + "' "
                + " AND ic_inventory_price.price_type=1 "
                + " AND " + strCondition_Today + " "
                + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " "
                + " AND ic_inventory_price.price_mode=0 " //price_mode = 0
                + " ORDER BY ic_inventory_price.price_mode DESC, ic_inventory_price.sale_type DESC, ic_inventory_price.transport_type DESC");

        // 4 = ราคาตามสูตร (ค้นหาระดับราคาลูกค้า)
        __QueryList.add("SELECT ar_customer.price_level "
                + "FROM ar_customer WHERE ar_customer.code = '" + strCustomerCode + "' ");

        // 5 = ราคาตามสูตร
        String strResultSaleType = "0";
        switch (Integer.parseInt(strSaleType)) {
            case 0: // ขายเชื่อ
                strResultSaleType += ",2";
                break;
            case 1: // ขายสด
                strResultSaleType += ",1";
                break;
            case 2: // ขายเชื่อ
                strResultSaleType += ",2";
                break;
        }

        String strResultVatType = "0";
        switch (strVatType) {
            case "Tax Excluded":
            case "ภาษีแยกนอก":
                strResultVatType += ",1";
                break;
            case "Tax Included":
            case "ภาษีรวมใน":
                strResultVatType += ",2";
            case "Zero Tax":
            case "ยกเว้นภาษี":
                strResultVatType += ",3";
                break;
        }

        __QueryList.add("SELECT * FROM ic_inventory_price_formula "
                + " WHERE ic_inventory_price_formula.ic_code='" + strICCode + "' "
                + " AND ic_inventory_price_formula.unit_code='" + strUnitCode + "' "
                + " AND ic_inventory_price_formula.sale_type IN (" + strResultSaleType + ") "
                + " AND COALESCE(ic_inventory_price_formula.tax_type, 0) IN (" + strResultVatType + ") "
                + " ORDER BY ic_inventory_price_formula.sale_type DESC");

        // 6 = ราคาตามบาร์โค้ด
//        __QueryList.add("SELECT ic_inventory_barcode.price "
//                + " FROM ic_inventory_barcode WHERE ic_inventory_barcode.barcode='" + strBarCode + "'");
        Integer get_last_price_type = 0;
        Integer ic_price_formula_control = 0;
        Integer lastPrice = -1;

        try {
            _routine __routine = new _routine(strDatabaseName, _global.FILE_CONFIG(strProvider));

            try {
//                String __strQuery = "SELECT barcode FROM ic_inventory_barcode WHERE barcode='" + strICCode + "' AND unit_code='" + strUnitCode + "'";
                String __strQuery = "SELECT barcode,price FROM ic_inventory_barcode WHERE ic_inventory_barcode.barcode='" + strICCode + "'";
                ResultSet __rsDataBarcode;
                __rsDataBarcode = __routine._excute(__strQuery, null);
                while (__rsDataBarcode.next()) {
                    strBarcode += (strBarcode.length() > 0) ? "," + __rsDataBarcode.getString("barcode") : __rsDataBarcode.getString("barcode");
                }
            } catch (Exception ex) {
                //ex.printStackTrace();
                __objResponse.put("err_msg", ex.getMessage());
            }

            try {
                String __strQUERY0 = "SELECT get_last_price_type, ic_price_formula_control FROM erp_option";
                ResultSet __rsData0;
                __rsData0 = __routine._excute(__strQUERY0, null);
                while (__rsData0.next()) {
                    get_last_price_type = __rsData0.getInt("get_last_price_type");
                    ic_price_formula_control = __rsData0.getInt("ic_price_formula_control");
                }
                __rsData0.close();
            } catch (SQLException ex) {
                //ex.printStackTrace();
                __objResponse.put("err_msg", ex.getMessage());
            }

            switch (get_last_price_type) {
                case 1: // ราคาขายล่าสุด
                    lastPrice = 7;
                    __QueryList.add("SELECT ic_trans_detail.price_exclude_vat, ic_trans_detail.price, "
                            + " (SELECT vat_type FROM ic_trans WHERE ic_trans.doc_no=ic_trans_detail.doc_no AND ic_trans.trans_flag=ic_trans_detail.trans_flag) AS vat_type "
                            + " FROM ic_trans_detail "
                            + " WHERE ic_trans_detail.cust_code='" + strCustomerCode + "' "
                            + " AND ic_trans_detail.item_code='" + strICCode + "' "
                            + " AND ic_trans_detail.unit_code='" + strUnitCode + "' "
                            + " AND ic_trans_detail.last_status=0 "
                            + " AND ic_trans_detail.trans_flag=44 "
                            + " AND ic_trans_detail.price_exclude_vat > 0 "
                            + " ORDER BY ic_trans_detail.doc_date DESC, ic_trans_detail.doc_time DESC LIMIT 1");
                    break;
                case 2: // ราคาขายเฉลี่ย
                    lastPrice = 7;
                    __QueryList.add("SELECT SUM(ic_trans_detail.price_exclude_vat)/COUNT(*) AS ic_trans_detail.price_exclude_vat "
                            + " FROM ic_trans_detail "
                            + " WHERE ic_trans_detail.cust_code='" + strCustomerCode + "'"
                            + " AND ic_trans_detail.item_code='" + strICCode + "' "
                            + " AND ic_trans_detail.unit_code='" + strUnitCode + "' "
                            + " AND ic_trans_detail.last_status=0 "
                            + " AND ic_trans_detail.trans_flag=44 "
                            + " AND ic_trans_detail.price_exclude_vat > 0 "
                            + " GROUP BY ic_trans_detail.item_code='" + strICCode + "' AND ic_trans_detail.unit_code='" + strUnitCode + "'");
                    break;
                default:
                    break;
            }

            // ลดตามกลุ่ม7
            __QueryList.add("select discount_word as discount from ic_inventory_barcode_price where "
                    + "barcode = (select barcode from ic_inventory_barcode where ic_code = '" + strICCode + "' and unit_code = '" + strUnitCode + "' limit 1) \n"
                    + "and customer_group = (SELECT ar_customer_detail.group_main FROM ar_customer_detail WHERE ar_customer_detail.ar_code='" + strCustomerCode + "') \n"
                    + strCondition_Today_barcodeDiscount + strCondition_QTY_barcodeDiscount
                    + "order by roworder desc limit 1");

            // ลดตามลูกค้า8
            __QueryList.add("SELECT roworder, ic_inventory_discount.discount "
                    + " FROM ic_inventory_discount "
                    + " WHERE ic_inventory_discount.ic_code='" + strICCode + "' "
                    + " AND ic_inventory_discount.unit_code='" + strUnitCode + "' "
                    + " AND ic_inventory_discount.cust_code='" + strCustomerCode + "' "
                    + " AND ic_inventory_discount.discount_type=2 "
                    + " AND " + strCondition_Today + " "
                    + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " "
                    + " ORDER BY line_number");

            // ลดตามกลุ่ม9
            __QueryList.add("SELECT roworder, ic_inventory_discount.discount "
                    + " FROM ic_inventory_discount "
                    + " WHERE ic_inventory_discount.ic_code='" + strICCode + "' "
                    + " AND ic_inventory_discount.unit_code='" + strUnitCode + "' "
                    + " AND ic_inventory_discount.cust_group_1=(SELECT ar_customer_detail.group_main FROM ar_customer_detail WHERE ar_customer_detail.ar_code='" + strCustomerCode + "') "
                    + " " + strSubWhere + " "
                    + " AND ic_inventory_discount.discount_type=1 "
                    + " AND " + strCondition_Today + " "
                    + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " "
                    + "ORDER BY roworder");

            // ลดทั่วไป10
            __QueryList.add("SELECT roworder, ic_inventory_discount.discount "
                    + " FROM ic_inventory_discount "
                    + " WHERE ic_inventory_discount.ic_code='" + strICCode + "' "
                    + " AND ic_inventory_discount.unit_code='" + strUnitCode + "' "
                    + " AND ic_inventory_discount.discount_type=0 "
                    + " AND " + strCondition_Today + " "
                    + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " ");

            JSONArray __JSONArr = new JSONArray();
            JSONObject tmpList = new JSONObject();
            Boolean __foundPrice = false;
            Boolean __foundByCondition = false;

            try {
                ResultSet __rsData1;
                __rsData1 = __routine._excute(__QueryList.get(0), null);
//                System.out.println(__QueryList.get(0));
                while (__rsData1.next()) {
                    __foundPrice = true;
                    __foundByCondition = true;
                    tmpList.put("price1", __rsData1.getString("sale_price1"));
                    tmpList.put("price2", __rsData1.getString("sale_price2"));
                    tmpList.put("price", __rsData1.getString("sale_price2"));
                    tmpList.put("mode", __rsData1.getString("price_mode"));
                    tmpList.put("roworder", __rsData1.getString("price_mode"));
                    tmpList.put("query", __QueryList.get(0));
                    tmpList.put("type", "1");
                }
                __rsData1.close();
            } catch (SQLException ex) {
                //ex.printStackTrace();
                __objResponse.put("err_msg", ex.getMessage());
            }

            if (!__foundByCondition) {
                try {
                    ResultSet __rsData2;
                    __rsData2 = __routine._excute(__QueryList.get(1), null);
                    //     System.out.println("__QueryList.get(1)" + __QueryList.get(1));
                    while (__rsData2.next()) {
                        __foundPrice = true;
                        __foundByCondition = true;
                        tmpList.put("price1", __rsData2.getString("sale_price1"));
                        tmpList.put("price2", __rsData2.getString("sale_price2"));
                        tmpList.put("price", __rsData2.getString("sale_price2"));
                        tmpList.put("mode", __rsData2.getString("price_mode"));
                        tmpList.put("roworder", __rsData2.getString("price_mode"));
                        tmpList.put("query", __QueryList.get(1));
                        tmpList.put("type", "2");
                    }
                    __rsData2.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            if (!__foundByCondition) {
                try {
                    ResultSet __rsData2;
                    __rsData2 = __routine._excute(__QueryList.get(2), null);
                    //  System.out.println("__QueryList.get(2)" + __QueryList.get(2));
                    while (__rsData2.next()) {
                        __foundPrice = true;
                        __foundByCondition = true;
                        tmpList.put("price1", __rsData2.getString("sale_price1"));
                        tmpList.put("price2", __rsData2.getString("sale_price2"));
                        tmpList.put("price", __rsData2.getString("sale_price2"));
                        tmpList.put("mode", __rsData2.getString("price_mode"));
                        tmpList.put("roworder", __rsData2.getString("price_mode"));
                        tmpList.put("query", __QueryList.get(2));
                        tmpList.put("type", "3");
                    }
                    __rsData2.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            if (!__foundByCondition) {
                try {
                    ResultSet __rsData2;
                    __rsData2 = __routine._excute(__QueryList.get(3), null);
                    // System.out.println("__QueryList.get(3) " + __QueryList.get(3));
                    while (__rsData2.next()) {
                        __foundPrice = true;
                        __foundByCondition = true;
                        tmpList.put("price1", __rsData2.getString("sale_price1"));
                        tmpList.put("price2", __rsData2.getString("sale_price2"));
                        tmpList.put("price", __rsData2.getString("sale_price2"));
                        tmpList.put("mode", __rsData2.getString("price_mode"));
                        tmpList.put("roworder", __rsData2.getString("price_mode"));
                        tmpList.put("query", __QueryList.get(3));
                        tmpList.put("type", "3");
                    }
                    __rsData2.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            if (!__foundPrice) {
                try {
                    Integer __priceLevel = 0;
                    ResultSet __rsData2;
                    __rsData2 = __routine._excute(__QueryList.get(4), null);
                    //  System.out.println("__QueryList.get(4)" + __QueryList.get(4));
                    while (__rsData2.next()) {
                        __priceLevel = __rsData2.getInt("price_level");
                    }
                    __rsData2.close();
                    __rsData2 = __routine._excute(__QueryList.get(5), null);
                    //  System.out.println("__QueryList.get(4)" + __QueryList.get(4));
                    while (__rsData2.next()) {
                        __foundPrice = true;
                        String strPriceStandard = __rsData2.getString("price_0");
                        String strFormula = strPriceStandard;
                        switch (__priceLevel) {
                            case 1:
                                strFormula = __rsData2.getString("price_1");
                                break;
                            case 2:
                                strFormula = __rsData2.getString("price_2");
                                break;
                            case 3:
                                strFormula = __rsData2.getString("price_3");
                                break;
                            case 4:
                                strFormula = __rsData2.getString("price_4");
                                break;
                            case 5:
                                strFormula = __rsData2.getString("price_5");
                                break;
                            case 6:
                                strFormula = __rsData2.getString("price_6");
                                break;
                            case 7:
                                strFormula = __rsData2.getString("price_7");
                                break;
                            case 8:
                                strFormula = __rsData2.getString("price_8");
                                break;
                            case 9:
                                strFormula = __rsData2.getString("price_9");
                                break;
                        }

                        String strResultPrice = __myGlobal._calcFormulaPrice(strQTY, strPriceStandard, (strFormula == null ? "" : strFormula));
                        tmpList.put("price", strResultPrice);
                        tmpList.put("price2", strResultPrice);
                        tmpList.put("mode", "6");
                        tmpList.put("roworder", "0");
                        tmpList.put("query", "strFormula " + strFormula);
                        tmpList.put("type", "6");
                    }
                    __rsData2.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            if (!__foundPrice && strBarcode.trim().length() > 0) {
                try {
                    // ค้นหาตามบาร์โค้ด
                    String[] sptBarcode = strBarcode.split(",");
                    JSONArray arrBarcode = new JSONArray();
                    for (String barcode : sptBarcode) {
                        ResultSet __rsData2;
//                        System.out.println("SELECT price FROM ic_inventory_barcode WHERE barcode='" + barcode + "'");
                        __rsData2 = __routine._excute("SELECT price FROM ic_inventory_barcode WHERE barcode='" + barcode + "'", null);
                        while (__rsData2.next()) {
                            __foundPrice = true;
                            JSONObject objBarcode = new JSONObject();
                            objBarcode.put("price", __rsData2.getString("price"));
                            objBarcode.put("price2", __rsData2.getString("price"));
                            objBarcode.put("mode", "5");
                            objBarcode.put("barcode", barcode);
                            objBarcode.put("roworder", "0");
                            objBarcode.put("type", "5");

                            arrBarcode.put(objBarcode);
                        }
                        __rsData2.close();
                    }
                    tmpList.put("data", arrBarcode);
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            if (!__foundPrice && lastPrice != -1) {
                try {
                    // ราคาขายล่าสุด หาร ราคาขายเฉลี่ย
                    ResultSet __rsData2;
                    __rsData2 = __routine._excute(__QueryList.get(lastPrice), null);
//                    System.out.println(__QueryList.get(lastPrice));
                    __rsData2.next();
                    while (__rsData2.next()) {
                        __foundPrice = true;
                        Integer _price = __rsData2.getInt("price_exclude_vat");

                        if (strVatType.equals("ภาษีรวมใน") || strVatType.equals("Tax Included")) {
                            _price = _price + (_price * (Integer.parseInt(strVatRate) / 100));
                        }

                        tmpList.put("price2", _price);
                        tmpList.put("mode", "5");
                        tmpList.put("roworder", "0");
                        tmpList.put("query", __QueryList.get(lastPrice));
                        tmpList.put("type", "7");
                    }
                    __rsData2.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            String strPriceStandard = "";
//            System.out.println(__QueryList.get(5));
            System.out.println("__QueryList.get(5)" + __QueryList.get(5));
            if (__foundPrice && ic_price_formula_control == 1) {
                try {
                    ResultSet __rsData2;
                    __rsData2 = __routine._excute(__QueryList.get(5), null);

                    while (__rsData2.next()) {
                        strPriceStandard = __rsData2.getString("price_0");
                        tmpList.put("stand_price", strPriceStandard);
                    }
                    __rsData2.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            // หาส่วนลด
            Boolean __foundDiscount = false;
            String __strDefaultDiscount = "";
            try {
                ResultSet __rsData3;
                __rsData3 = __routine._excute(__QueryList.get(7), null);
//                System.out.println(__QueryList.get(7));
                while (__rsData3.next()) {
                    __foundDiscount = true;
                    __strDefaultDiscount = __rsData3.getString("discount");
                    tmpList.put("defaultDiscount", __strDefaultDiscount);
                }
                __rsData3.close();
            } catch (SQLException ex) {
                __objResponse.put("err_msg", ex.getMessage());
            }

            if (!__foundDiscount) {
                try {
                    ResultSet __rsData3;
                    __rsData3 = __routine._excute(__QueryList.get(8), null);
//                    System.out.println(__QueryList.get(8));
                    while (__rsData3.next()) {
                        __foundDiscount = true;
                        __strDefaultDiscount = __rsData3.getString("discount");
                        tmpList.put("defaultDiscount", __strDefaultDiscount);
                    }
                    __rsData3.close();
                } catch (SQLException ex) {
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            // หาส่วนลดต่อไป..
            if (!__foundDiscount) {
                try {
                    ResultSet __rsData3;
                    __rsData3 = __routine._excute(__QueryList.get(9), null);
//                    System.out.println(__QueryList.get(9));
                    while (__rsData3.next()) {
                        __foundDiscount = true;
                        __strDefaultDiscount = __rsData3.getString("discount");
                        tmpList.put("defaultDiscount", __strDefaultDiscount);
                    }
                    __rsData3.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

//            System.out.println("tmpList " + tmpList);
            __JSONArr.put(tmpList);

            __objResponse.put("success", true);
            __objResponse.put("data", __JSONArr);
        } catch (Exception ex) {
            //ex.printStackTrace();
            __objResponse.put("err_msg", ex.getMessage());
        }
        return __objResponse;
    }

    @GET
    @Path("/getPriceSales") // ดึงราคา
    public Response getPriceSales(
            @QueryParam("ic_code") String strICCode,
            @QueryParam("unit_code") String strUnitCode,
            @QueryParam("qty") String strQTY,
            @QueryParam("cust_code") String strCustomerCode,
            @QueryParam("vat_rate") String strVatRate) {

        String strProvider = "DEMO";
        String strDatabaseName = "demo1";
        String strVatType = "ภาษีรวมใน";
        String strSaleType = "0";
        JSONObject __objResponse = new JSONObject();
        __objResponse.put("success", false);
        String strBarcode = "";

        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);

        String strCondition_Today = " ('" + ft.format(new Date()) + "' >= from_date  AND '" + ft.format(new Date()) + "' <= to_date) ";
        String strCondition_QTY = " ('" + (strQTY.equals("0") ? "1" : strQTY) + "' >= from_qty AND '" + (strQTY.equals("0") ? "1" : strQTY) + "' <= to_qty) ";

        String strCondition_Today_barcodeDiscount = " AND ('" + ft.format(new Date()) + "' >= date_begin  AND '" + ft.format(new Date()) + "' <= date_end) ";
        String strCondition_QTY_barcodeDiscount = " AND ('" + (strQTY.equals("0") ? "1" : strQTY) + "' >= qty_begin AND '" + (strQTY.equals("0") ? "1" : strQTY) + "' <= qty_end) ";

        String strField_Price = strVatType.equals("ภาษีแยกนอก") || strVatType.equals("Tax Excluded") ? "sale_price1" : "sale_price2";
        String strSaleFilterType = Integer.parseInt(strSaleType) == 0 || Integer.parseInt(strSaleType) == 2 ? "2" : "1";
        String strCondition_SaleType = " AND sale_type IN (0," + strSaleFilterType + ") ";
        String strCondition_TransportType = "";

        // 0 = ราคาตามลูกค้า , 1 = ราคาตามกลุ่มลูกค้า
        // 2 = ราคาขายทั่วไป , 3 = ราคามาตราฐาน
        // 4 = ราคาตามสูตร , 5 = ราคาตามสูตร
        // 6 = ราคาตามบาร์โค้ด
        myglobal __myGlobal = new myglobal();
        List<String> __QueryList = new ArrayList<>();
        String strSubWhere = " AND ("
                + " (cust_group_2   = (SELECT group_sub_1 FROM ar_customer_detail  WHERE  ar_customer_detail.ar_code = '" + strCustomerCode + "')) "
                + " OR (cust_group_2   = (SELECT group_sub_2 FROM ar_customer_detail  WHERE  ar_customer_detail.ar_code = '" + strCustomerCode + "')) "
                + " OR (cust_group_2   = (SELECT group_sub_3 FROM ar_customer_detail  WHERE  ar_customer_detail.ar_code = '" + strCustomerCode + "')) "
                + " OR (cust_group_2   = (SELECT group_sub_4 FROM ar_customer_detail  WHERE  ar_customer_detail.ar_code = '" + strCustomerCode + "')) "
                + " OR (COALESCE(cust_group_2, '')= '')"
                + ") ";

        // 0 = ราคาตามลูกค้า
        __QueryList.add("SELECT roworder ,ic_inventory_price.sale_price1, ic_inventory_price.sale_price2,ic_inventory_price.price_mode "
                + " FROM ic_inventory_price "
                + " WHERE ic_inventory_price.ic_code='" + strICCode + "' "
                + " AND ic_inventory_price.unit_code='" + strUnitCode + "' "
                + " AND ic_inventory_price.cust_code='" + strCustomerCode + "' "
                + " AND ic_inventory_price.price_type=3 "
                + " AND " + strCondition_Today + " "
                + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " "
                + " ORDER BY ic_inventory_price.price_mode DESC, ic_inventory_price.sale_type DESC, ic_inventory_price.transport_type DESC");

        // 1 = ราคาตามกลุ่มลูกค้า
        __QueryList.add("SELECT roworder ,ic_inventory_price.sale_price1, ic_inventory_price.sale_price2,ic_inventory_price.price_mode "
                + " FROM ic_inventory_price "
                + " WHERE ic_inventory_price.ic_code='" + strICCode + "' "
                + " AND ic_inventory_price.unit_code='" + strUnitCode + "' "
                + " AND ic_inventory_price.cust_group_1=(SELECT ar_customer_detail.group_main FROM ar_customer_detail WHERE ar_customer_detail.ar_code='" + strCustomerCode + "') "
                + " " + strSubWhere + " "
                + " AND ic_inventory_price.price_type=2 "
                + " AND " + strCondition_Today + " "
                + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " "
                + " ORDER BY ic_inventory_price.price_mode DESC, ic_inventory_price.sale_type DESC, ic_inventory_price.transport_type DESC, ic_inventory_price.cust_group_2");

        // 2 = ราคาขายทั่วไป
        __QueryList.add("SELECT roworder ,ic_inventory_price.sale_price1, ic_inventory_price.sale_price2,ic_inventory_price.price_mode "
                + " FROM ic_inventory_price "
                + " WHERE ic_inventory_price.ic_code='" + strICCode + "' "
                + " AND ic_inventory_price.unit_code='" + strUnitCode + "' "
                + " AND ic_inventory_price.price_type=1 "
                + " AND " + strCondition_Today + " "
                + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " "
                + " AND ic_inventory_price.price_mode=1 " // price_mode = 1
                + " ORDER BY ic_inventory_price.price_mode DESC, ic_inventory_price.sale_type DESC, ic_inventory_price.transport_type DESC");

        // 3 = ราคามาตราฐาน
        __QueryList.add("SELECT roworder ,ic_inventory_price.sale_price1, ic_inventory_price.sale_price2,ic_inventory_price.price_mode "
                + " FROM ic_inventory_price "
                + " WHERE ic_inventory_price.ic_code='" + strICCode + "' "
                + " AND ic_inventory_price.unit_code='" + strUnitCode + "' "
                + " AND ic_inventory_price.price_type=1 "
                + " AND " + strCondition_Today + " "
                + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " "
                + " AND ic_inventory_price.price_mode=0 " //price_mode = 0
                + " ORDER BY ic_inventory_price.price_mode DESC, ic_inventory_price.sale_type DESC, ic_inventory_price.transport_type DESC");

        // 4 = ราคาตามสูตร (ค้นหาระดับราคาลูกค้า)
        __QueryList.add("SELECT ar_customer.price_level "
                + "FROM ar_customer WHERE ar_customer.code = '" + strCustomerCode + "' ");

        // 5 = ราคาตามสูตร
        String strResultSaleType = "0";
        switch (Integer.parseInt(strSaleType)) {
            case 0: // ขายเชื่อ
                strResultSaleType += ",2";
                break;
            case 1: // ขายสด
                strResultSaleType += ",1";
                break;
        }

        String strResultVatType = "0";
        switch (strVatType) {
            case "Tax Excluded":
            case "ภาษีแยกนอก":
                strResultVatType += ",1";
                break;
            case "Tax Included":
            case "ภาษีรวมใน":
                strResultVatType += ",2";
            case "Zero Tax":
            case "ยกเว้นภาษี":
                strResultVatType += ",3";
                break;
        }

        __QueryList.add("SELECT * FROM ic_inventory_price_formula "
                + " WHERE ic_inventory_price_formula.ic_code='" + strICCode + "' "
                + " AND ic_inventory_price_formula.unit_code='" + strUnitCode + "' "
                + " AND ic_inventory_price_formula.sale_type IN (" + strResultSaleType + ") "
                + " AND COALESCE(ic_inventory_price_formula.tax_type, 0) IN (" + strResultVatType + ") "
                + " ORDER BY ic_inventory_price_formula.sale_type DESC");

        // 6 = ราคาตามบาร์โค้ด
//        __QueryList.add("SELECT ic_inventory_barcode.price "
//                + " FROM ic_inventory_barcode WHERE ic_inventory_barcode.barcode='" + strBarCode + "'");
        Integer get_last_price_type = 0;
        Integer ic_price_formula_control = 0;
        Integer lastPrice = -1;

        try {
            _routine __routine = new _routine(strDatabaseName, _global.FILE_CONFIG(strProvider));

            try {
//                String __strQuery = "SELECT barcode FROM ic_inventory_barcode WHERE barcode='" + strICCode + "' AND unit_code='" + strUnitCode + "'";
                String __strQuery = "SELECT barcode,price FROM ic_inventory_barcode WHERE ic_inventory_barcode.barcode='" + strICCode + "'";
                ResultSet __rsDataBarcode;
                __rsDataBarcode = __routine._excute(__strQuery, null);
                while (__rsDataBarcode.next()) {
                    strBarcode += (strBarcode.length() > 0) ? "," + __rsDataBarcode.getString("barcode") : __rsDataBarcode.getString("barcode");
                }
            } catch (Exception ex) {
                //ex.printStackTrace();
                __objResponse.put("err_msg", ex.getMessage());
            }

            try {
                String __strQUERY0 = "SELECT get_last_price_type, ic_price_formula_control FROM erp_option";
                ResultSet __rsData0;
                __rsData0 = __routine._excute(__strQUERY0, null);
                while (__rsData0.next()) {
                    get_last_price_type = __rsData0.getInt("get_last_price_type");
                    ic_price_formula_control = __rsData0.getInt("ic_price_formula_control");
                }
                __rsData0.close();
            } catch (SQLException ex) {
                //ex.printStackTrace();
                __objResponse.put("err_msg", ex.getMessage());
            }

            switch (get_last_price_type) {
                case 1: // ราคาขายล่าสุด
                    lastPrice = 7;
                    __QueryList.add("SELECT ic_trans_detail.price_exclude_vat, ic_trans_detail.price, "
                            + " (SELECT vat_type FROM ic_trans WHERE ic_trans.doc_no=ic_trans_detail.doc_no AND ic_trans.trans_flag=ic_trans_detail.trans_flag) AS vat_type "
                            + " FROM ic_trans_detail "
                            + " WHERE ic_trans_detail.cust_code='" + strCustomerCode + "' "
                            + " AND ic_trans_detail.item_code='" + strICCode + "' "
                            + " AND ic_trans_detail.unit_code='" + strUnitCode + "' "
                            + " AND ic_trans_detail.last_status=0 "
                            + " AND ic_trans_detail.trans_flag=44 "
                            + " AND ic_trans_detail.price_exclude_vat > 0 "
                            + " ORDER BY ic_trans_detail.doc_date DESC, ic_trans_detail.doc_time DESC LIMIT 1");
                    break;
                case 2: // ราคาขายเฉลี่ย
                    lastPrice = 7;
                    __QueryList.add("SELECT SUM(ic_trans_detail.price_exclude_vat)/COUNT(*) AS ic_trans_detail.price_exclude_vat "
                            + " FROM ic_trans_detail "
                            + " WHERE ic_trans_detail.cust_code='" + strCustomerCode + "'"
                            + " AND ic_trans_detail.item_code='" + strICCode + "' "
                            + " AND ic_trans_detail.unit_code='" + strUnitCode + "' "
                            + " AND ic_trans_detail.last_status=0 "
                            + " AND ic_trans_detail.trans_flag=44 "
                            + " AND ic_trans_detail.price_exclude_vat > 0 "
                            + " GROUP BY ic_trans_detail.item_code='" + strICCode + "' AND ic_trans_detail.unit_code='" + strUnitCode + "'");
                    break;
                default:
                    break;
            }

            // ลดตามกลุ่ม7
            __QueryList.add("select discount_word as discount from ic_inventory_barcode_price where "
                    + "barcode = (select barcode from ic_inventory_barcode where ic_code = '" + strICCode + "' and unit_code = '" + strUnitCode + "' limit 1) \n"
                    + "and customer_group = (SELECT ar_customer_detail.group_main FROM ar_customer_detail WHERE ar_customer_detail.ar_code='" + strCustomerCode + "') \n"
                    + strCondition_Today_barcodeDiscount + strCondition_QTY_barcodeDiscount
                    + "order by roworder desc limit 1");

            // ลดตามลูกค้า8
            __QueryList.add("SELECT roworder, ic_inventory_discount.discount "
                    + " FROM ic_inventory_discount "
                    + " WHERE ic_inventory_discount.ic_code='" + strICCode + "' "
                    + " AND ic_inventory_discount.unit_code='" + strUnitCode + "' "
                    + " AND ic_inventory_discount.cust_code='" + strCustomerCode + "' "
                    + " AND ic_inventory_discount.discount_type=2 "
                    + " AND " + strCondition_Today + " "
                    + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " "
                    + " ORDER BY line_number");

            // ลดตามกลุ่ม9
            __QueryList.add("SELECT roworder, ic_inventory_discount.discount "
                    + " FROM ic_inventory_discount "
                    + " WHERE ic_inventory_discount.ic_code='" + strICCode + "' "
                    + " AND ic_inventory_discount.unit_code='" + strUnitCode + "' "
                    + " AND ic_inventory_discount.cust_group_1=(SELECT ar_customer_detail.group_main FROM ar_customer_detail WHERE ar_customer_detail.ar_code='" + strCustomerCode + "') "
                    + " " + strSubWhere + " "
                    + " AND ic_inventory_discount.discount_type=1 "
                    + " AND " + strCondition_Today + " "
                    + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " "
                    + "ORDER BY roworder");

            // ลดทั่วไป10
            __QueryList.add("SELECT roworder, ic_inventory_discount.discount "
                    + " FROM ic_inventory_discount "
                    + " WHERE ic_inventory_discount.ic_code='" + strICCode + "' "
                    + " AND ic_inventory_discount.unit_code='" + strUnitCode + "' "
                    + " AND ic_inventory_discount.discount_type=0 "
                    + " AND " + strCondition_Today + " "
                    + " AND " + strCondition_QTY + " " + strCondition_SaleType + " " + strCondition_TransportType + " ");

            JSONArray __JSONArr = new JSONArray();
            JSONObject tmpList = new JSONObject();
            Boolean __foundPrice = false;
            Boolean __foundByCondition = false;

            try {
                ResultSet __rsData1;
                __rsData1 = __routine._excute(__QueryList.get(0), null);
                System.out.println(__QueryList.get(0));
                while (__rsData1.next()) {
                    __foundPrice = true;
                    __foundByCondition = true;
                    tmpList.put("price1", __rsData1.getString("sale_price1"));
                    tmpList.put("price2", __rsData1.getString("sale_price2"));
                    tmpList.put("price", __rsData1.getString("sale_price2"));
                    tmpList.put("mode", __rsData1.getString("price_mode"));
                    tmpList.put("roworder", __rsData1.getString("price_mode"));
                    tmpList.put("query", __QueryList.get(0));
                    tmpList.put("type", "1");
                }
                __rsData1.close();
            } catch (SQLException ex) {
                //ex.printStackTrace();
                __objResponse.put("err_msg", ex.getMessage());
            }

            if (!__foundByCondition) {
                try {
                    ResultSet __rsData2;
                    __rsData2 = __routine._excute(__QueryList.get(1), null);
                    System.out.println(__QueryList.get(1));
                    while (__rsData2.next()) {
                        __foundPrice = true;
                        __foundByCondition = true;
                        tmpList.put("price1", __rsData2.getString("sale_price1"));
                        tmpList.put("price2", __rsData2.getString("sale_price2"));
                        tmpList.put("price", __rsData2.getString("sale_price2"));
                        tmpList.put("mode", __rsData2.getString("price_mode"));
                        tmpList.put("roworder", __rsData2.getString("price_mode"));
                        tmpList.put("query", __QueryList.get(1));
                        tmpList.put("type", "2");
                    }
                    __rsData2.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            if (!__foundByCondition) {
                try {
                    ResultSet __rsData2;
                    __rsData2 = __routine._excute(__QueryList.get(2), null);
                    //   System.out.println(__QueryList.get(2));
                    while (__rsData2.next()) {
                        __foundPrice = true;
                        __foundByCondition = true;
                        tmpList.put("price1", __rsData2.getString("sale_price1"));
                        tmpList.put("price2", __rsData2.getString("sale_price2"));
                        tmpList.put("price", __rsData2.getString("sale_price2"));
                        tmpList.put("mode", __rsData2.getString("price_mode"));
                        tmpList.put("roworder", __rsData2.getString("price_mode"));
                        tmpList.put("query", __QueryList.get(2));
                        tmpList.put("type", "3");
                    }
                    __rsData2.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            if (!__foundByCondition) {
                try {
                    ResultSet __rsData2;
                    __rsData2 = __routine._excute(__QueryList.get(3), null);
                    //   System.out.println(__QueryList.get(3));
                    while (__rsData2.next()) {
                        __foundPrice = true;
                        __foundByCondition = true;
                        tmpList.put("price1", __rsData2.getString("sale_price1"));
                        tmpList.put("price2", __rsData2.getString("sale_price2"));
                        tmpList.put("price", __rsData2.getString("sale_price2"));
                        tmpList.put("mode", __rsData2.getString("price_mode"));
                        tmpList.put("roworder", __rsData2.getString("price_mode"));
                        tmpList.put("query", __QueryList.get(3));
                        tmpList.put("type", "3");
                    }
                    __rsData2.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            if (!__foundPrice) {
                try {
                    Integer __priceLevel = 0;
                    ResultSet __rsData2;
                    __rsData2 = __routine._excute(__QueryList.get(4), null);
                    //  System.out.println(__QueryList.get(4));
                    while (__rsData2.next()) {
                        __priceLevel = __rsData2.getInt("price_level");
                    }
                    __rsData2.close();
                    __rsData2 = __routine._excute(__QueryList.get(5), null);
                    //  System.out.println(__QueryList.get(4));
                    while (__rsData2.next()) {
                        __foundPrice = true;
                        String strPriceStandard = __rsData2.getString("price_0");
                        String strFormula = strPriceStandard;
                        switch (__priceLevel) {
                            case 1:
                                strFormula = __rsData2.getString("price_1");
                                break;
                            case 2:
                                strFormula = __rsData2.getString("price_2");
                                break;
                            case 3:
                                strFormula = __rsData2.getString("price_3");
                                break;
                            case 4:
                                strFormula = __rsData2.getString("price_4");
                                break;
                            case 5:
                                strFormula = __rsData2.getString("price_5");
                                break;
                            case 6:
                                strFormula = __rsData2.getString("price_6");
                                break;
                            case 7:
                                strFormula = __rsData2.getString("price_7");
                                break;
                            case 8:
                                strFormula = __rsData2.getString("price_8");
                                break;
                            case 9:
                                strFormula = __rsData2.getString("price_9");
                                break;
                        }

                        String strResultPrice = __myGlobal._calcFormulaPrice(strQTY, strPriceStandard, (strFormula == null ? "" : strFormula));
                        tmpList.put("price", strResultPrice);
                        tmpList.put("price2", strResultPrice);
                        tmpList.put("mode", "6");
                        tmpList.put("roworder", "0");
                        tmpList.put("query", "strFormula " + strFormula);
                        tmpList.put("type", "6");
                    }
                    __rsData2.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            if (!__foundPrice && strBarcode.trim().length() > 0) {
                try {
                    // ค้นหาตามบาร์โค้ด
                    String[] sptBarcode = strBarcode.split(",");
                    JSONArray arrBarcode = new JSONArray();
                    for (String barcode : sptBarcode) {
                        ResultSet __rsData2;
                        //  System.out.println("SELECT price FROM ic_inventory_barcode WHERE barcode='" + barcode + "'");
                        __rsData2 = __routine._excute("SELECT price FROM ic_inventory_barcode WHERE barcode='" + barcode + "'", null);
                        while (__rsData2.next()) {
                            __foundPrice = true;
                            JSONObject objBarcode = new JSONObject();
                            objBarcode.put("price", __rsData2.getString("price"));
                            objBarcode.put("price2", __rsData2.getString("price"));
                            objBarcode.put("mode", "5");
                            objBarcode.put("barcode", barcode);
                            objBarcode.put("roworder", "0");
                            objBarcode.put("type", "5");

                            arrBarcode.put(objBarcode);
                        }
                        __rsData2.close();
                    }
                    tmpList.put("data", arrBarcode);
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            if (!__foundPrice && lastPrice != -1) {
                try {
                    // ราคาขายล่าสุด หาร ราคาขายเฉลี่ย
                    ResultSet __rsData2;
                    __rsData2 = __routine._excute(__QueryList.get(lastPrice), null);
                    //    System.out.println(__QueryList.get(lastPrice));
                    __rsData2.next();
                    while (__rsData2.next()) {
                        __foundPrice = true;
                        Integer _price = __rsData2.getInt("price_exclude_vat");

                        if (strVatType.equals("ภาษีรวมใน") || strVatType.equals("Tax Included")) {
                            _price = _price + (_price * (Integer.parseInt(strVatRate) / 100));
                        }

                        tmpList.put("price2", _price);
                        tmpList.put("mode", "5");
                        tmpList.put("roworder", "0");
                        tmpList.put("query", __QueryList.get(lastPrice));
                        tmpList.put("type", "7");
                    }
                    __rsData2.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            String strPriceStandard = "";
            //    System.out.println(__QueryList.get(5));
            //    System.out.println(__QueryList.get(5));
            if (__foundPrice && ic_price_formula_control == 1) {
                try {
                    ResultSet __rsData2;
                    __rsData2 = __routine._excute(__QueryList.get(5), null);

                    while (__rsData2.next()) {
                        strPriceStandard = __rsData2.getString("price_0");
                        tmpList.put("stand_price", strPriceStandard);
                    }
                    __rsData2.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            // หาส่วนลด
            Boolean __foundDiscount = false;
            String __strDefaultDiscount = "";
            try {
                ResultSet __rsData3;
                __rsData3 = __routine._excute(__QueryList.get(7), null);
                //    System.out.println(__QueryList.get(7));
                while (__rsData3.next()) {
                    __foundDiscount = true;
                    __strDefaultDiscount = __rsData3.getString("discount");
                    tmpList.put("defaultDiscount", __strDefaultDiscount);
                }
                __rsData3.close();
            } catch (SQLException ex) {
                __objResponse.put("err_msg", ex.getMessage());
            }

            if (!__foundDiscount) {
                try {
                    ResultSet __rsData3;
                    __rsData3 = __routine._excute(__QueryList.get(8), null);
                    //     System.out.println(__QueryList.get(8));
                    while (__rsData3.next()) {
                        __foundDiscount = true;
                        __strDefaultDiscount = __rsData3.getString("discount");
                        tmpList.put("defaultDiscount", __strDefaultDiscount);
                    }
                    __rsData3.close();
                } catch (SQLException ex) {
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }

            // หาส่วนลดต่อไป..
            if (!__foundDiscount) {
                try {
                    ResultSet __rsData3;
                    __rsData3 = __routine._excute(__QueryList.get(9), null);
                    //  System.out.println(__QueryList.get(9));
                    while (__rsData3.next()) {
                        __foundDiscount = true;
                        __strDefaultDiscount = __rsData3.getString("discount");
                        tmpList.put("defaultDiscount", __strDefaultDiscount);
                    }
                    __rsData3.close();
                } catch (SQLException ex) {
                    //ex.printStackTrace();
                    __objResponse.put("err_msg", ex.getMessage());
                }
            }
            __JSONArr.put(tmpList);
            __objResponse.put("success", true);
            __objResponse.put("data", __JSONArr);
        } catch (Exception ex) {
            //ex.printStackTrace();
            __objResponse.put("err_msg", ex.getMessage());
        }
        return Response.ok(String.valueOf(__objResponse), MediaType.APPLICATION_JSON).build();
    }

    public static JSONArray _convertResultSetIntoJSON(ResultSet resultSet) throws Exception {
        JSONArray jsonArray = new JSONArray();
        while (resultSet.next()) {
            int total_rows = resultSet.getMetaData().getColumnCount();
            JSONObject obj = new JSONObject();
            for (int i = 0; i < total_rows; i++) {
                String columnName = resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase();
                Object columnValue = resultSet.getObject(i + 1);
                // if value in DB is null, then we set it to default value
                if (columnValue == null) {
                    columnValue = "null";
                }
                /*
                Next if block is a hack. In case when in db we have values like price and price1 there's a bug in jdbc - 
                both this names are getting stored as price in ResulSet. Therefore when we store second column value,
                we overwrite original value of price. To avoid that, i simply add 1 to be consistent with DB.
                 */
                if (obj.has(columnName)) {
                    columnName += "1";
                }
                obj.put(columnName, columnValue);
            }
            jsonArray.put(obj);
        }
        return jsonArray;
    }

}
