val jul_file = sc.textFile("/home/vagrant/datos/NASA_access_log_Jul95")
val aug_file = sc.textFile("/home/vagrant/datos/NASA_access_log_Aug95")

val jul_tmp1 = jul_file.map(x => x.replace(" ","*"))
val jul_tmp2 = jul_tmp1.map(x => x.replace("*-*-*[","|"))
val jul_tmp3 = jul_tmp2.map(x => x.replace("]*\"","|"))
val jul_tmp4 = jul_tmp3.map(x => x.replace("\"*","|"))
val jul_tmp5 = jul_tmp4.map(x => x.replace("GET*/","GET /"))
val jul_tmp6 = jul_tmp5.map(x => x.replace("*HTTP/"," HTTP/"))
val jul_tmp7 = jul_tmp6.map(x => x.replace("*-","|0"))
val jul_tmp8 = jul_tmp7.map(x => x.replace("*","|"))
val jul_tmp9 = jul_tmp8.filter(x => x.contains("|"));

val aug_tmp1 = aug_file.map(x => x.replace(" ","*"))
val aug_tmp2 = aug_tmp1.map(x => x.replace("*-*-*[","|"))
val aug_tmp3 = aug_tmp2.map(x => x.replace("]*\"","|"))
val aug_tmp4 = aug_tmp3.map(x => x.replace("\"*","|"))
val aug_tmp5 = aug_tmp4.map(x => x.replace("GET*/","GET /"))
val aug_tmp6 = aug_tmp5.map(x => x.replace("*HTTP/"," HTTP/"))
val aug_tmp7 = aug_tmp6.map(x => x.replace("*-","|0"))
val aug_tmp8 = aug_tmp7.map(x => x.replace("*","|"))
val aug_tmp9 = aug_tmp8.filter(x => x.contains("|"));

val union_tmp = jul_tmp9.union(aug_tmp9)

union_tmp.coalesce(1).saveAsTextFile("/home/vagrant/datos/NASA_DADOS")

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val nasa_file = sc.textFile("/home/vagrant/datos/NASA_DADOS/part-00000").map(_.split('|').map(_.trim))

case class class_nasa(
Host:String,
TimeStamp:String,
TimeZone:String,
Requisicao:String,
Codigo_HTTP:String,
Total_Bytes:String)

val nasa_dados = nasa_file.map(a=>class_nasa(a(0),a(1),a(2),a(3),a(4),a(5))).toDF

nasa_dados.registerTempTable("nasa_table")

val out = sqlContext.sql("select * from nasa_table")
out.printSchema

sqlContext.sql("select count(*) from nasa_table").show

sqlContext.sql("select count(d.Host) as Hosts_Unicos from (select Host, count(Host) from nasa_table group by Host) as d").show

sqlContext.sql("select count(Codigo_HTTP) as Erros_404 from nasa_table where Codigo_HTTP = '404'").show

sqlContext.sql("select d.Host as URL_Top5, d.qtd as Qtd_Erros_404 from (select Host, count(Host) as qtd from nasa_table where Codigo_HTTP = '404' group by Host) as d order by d.qtd desc limit 5").show

sqlContext.sql("select d.dia as Dia, count(d.dia) as Qtd_Erros_404 from (select substr(TimeStamp,1,11) as dia from nasa_table where Codigo_HTTP = '404') as d group by d.dia order by d.dia asc").show(100)

sqlContext.sql("select cast(sum(Total_Bytes) as bigint) as Total_Bytes from nasa_table").show
