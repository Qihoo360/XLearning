package net.qihoo.xlearning.conf;

import net.qihoo.xlearning.common.TextMultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

public class XLearningConfiguration extends YarnConfiguration {

  private static final String XLEARNING_DEFAULT_XML_FILE = "xlearning-default.xml";

  private static final String XLEARNING_SITE_XML_FILE = "xlearning-site.xml";

  static {
    YarnConfiguration.addDefaultResource(XLEARNING_DEFAULT_XML_FILE);
    YarnConfiguration.addDefaultResource(XLEARNING_SITE_XML_FILE);
  }

  public XLearningConfiguration() {
    super();
  }

  public XLearningConfiguration(Configuration conf) {
    super(conf);
  }

  /**
   * Configuration used in Client
   */
  public static final String DEFAULT_XLEARNING_APP_TYPE = "XLEARNING";

  public static final String XLEARNING_STAGING_DIR = "xlearning.staging.dir";

  public static final String DEFAULT_XLEARNING_STAGING_DIR = "/tmp/XLearning/staging";

  public static final String XLEARNING_CONTAINER_EXTRAENV = "xlearning.container.extraEnv";

  public static final String XLEARNING_LOG_PULL_INTERVAL = "xlearning.log.pull.interval";

  public static final int DEFAULT_XLEARNING_LOG_PULL_INTERVAL = 10000;

  public static final String XLEARNING_USER_CLASSPATH_FIRST = "xlearning.user.classpath.first";

  public static final boolean DEFAULT_XLEARNING_USER_CLASSPATH_FIRST = true;

  public static final String XLEARNING_HOST_LOCAL_ENABLE = "xlearning.host.local.enable";

  public static final boolean DEFAULT_XLEARNING_HOST_LOCAL_ENABLE = false;

  public static final String XLEARNING_AM_NODELABELEXPRESSION = "xlearning.am.nodeLabelExpression";

  public static final String XLEARNING_WORKER_NODELABELEXPRESSION = "xlearning.worker.nodeLabelExpression";

  public static final String XLEARNING_PS_NODELABELEXPRESSION = "xlearning.ps.nodeLabelExpression";

  public static final String XLEARNING_REPORT_CONTAINER_STATUS = "xlearning.report.container.status";

  public static final boolean DEFAULT_XLEARNING_REPORT_CONTAINER_STATUS = true;

  public static final String XLEARNING_CONTAINER_MEM_USAGE_WARN_FRACTION = "xlearning.container.mem.usage.warn.fraction";

  public static final Double DEFAULT_XLEARNING_CONTAINER_MEM_USAGE_WARN_FRACTION = 0.70;

  public static final String XLEARNING_AM_MEMORY = "xlearning.am.memory";

  public static final int DEFAULT_XLEARNING_AM_MEMORY = 1024;

  public static final String XLEARNING_AM_CORES = "xlearning.am.cores";

  public static final int DEFAULT_XLEARNING_AM_CORES = 1;

  public static final String XLEARNING_WORKER_MEMORY = "xlearning.worker.memory";

  public static final int DEFAULT_XLEARNING_WORKER_MEMORY = 1024;

  public static final String XLEARNING_WORKER_VCORES = "xlearning.worker.cores";

  public static final int DEFAULT_XLEARNING_WORKER_VCORES = 1;

  public static final String XLEARNING_WORKER_NUM = "xlearning.worker.num";

  public static final int DEFAULT_XLEARNING_WORKER_NUM = 1;

  public static final String XLEARNING_CHIEF_WORKER_MEMORY = "xlearning.chief.worker.memory";

  public static final String XLEARNING_EVALUATOR_WORKER_MEMORY = "xlearning.evaluator.worker.memory";

  public static final String XLEARNING_PS_MEMORY = "xlearning.ps.memory";

  public static final int DEFAULT_XLEARNING_PS_MEMORY = 1024;

  public static final String XLEARNING_PS_VCORES = "xlearning.ps.cores";

  public static final int DEFAULT_XLEARNING_PS_VCORES = 1;

  public static final String XLEARNING_PS_NUM = "xlearning.ps.num";

  public static final int DEFAULT_XLEARNING_PS_NUM = 0;

  public static final String XLEARNING_WORKER_MEM_AUTO_SCALE = "xlearning.worker.mem.autoscale";

  public static final Double DEFAULT_XLEARNING_WORKER_MEM_AUTO_SCALE = 0.5;

  public static final String XLEARNING_PS_MEM_AUTO_SCALE = "xlearning.ps.mem.autoscale";

  public static final Double DEFAULT_XLEARNING_PS_MEM_AUTO_SCALE = 0.2;

  public static final String XLEARNING_APP_MAX_ATTEMPTS = "xlearning.app.max.attempts";

  public static final int DEFAULT_XLEARNING_APP_MAX_ATTEMPTS = 1;

  public static final String XLEARNING_MODE_SINGLE = "xlearning.mode.single";

  public static Boolean DEFAULT_XLEARNING_MODE_SINGLE = false;

  public static final String XLEARNING_TF_EVALUATOR = "xlearning.tf.evaluator";

  public static Boolean DEFAULT_XLEARNING_TF_EVALUATOR = false;

  public static final String XLEARNING_APP_QUEUE = "xlearning.app.queue";

  public static final String DEFAULT_XLEARNING_APP_QUEUE = "DEFAULT";

  public static final String XLEARNING_APP_PRIORITY = "xlearning.app.priority";

  public static final int DEFAULT_XLEARNING_APP_PRIORITY = 3;

  public static final String XLEARNING_OUTPUT_LOCAL_DIR = "xlearning.output.local.dir";

  public static final String DEFAULT_XLEARNING_OUTPUT_LOCAL_DIR = "output";

  public static final String XLEARNING_INPUTF0RMAT_CLASS = "xlearning.inputformat.class";

  public static final Class<? extends InputFormat> DEFAULT_XLEARNING_INPUTF0RMAT_CLASS = org.apache.hadoop.mapred.TextInputFormat.class;

  public static final String XLEARNING_OUTPUTFORMAT_CLASS = "xlearning.outputformat.class";

  public static final Class<? extends OutputFormat> DEFAULT_XLEARNING_OUTPUTF0RMAT_CLASS = TextMultiOutputFormat.class;

  public static final String XLEARNING_INPUTFILE_RENAME = "xlearning.inputfile.rename";

  public static final Boolean DEFAULT_XLEARNING_INPUTFILE_RENAME = false;

  public static final String XLEARNING_INPUT_STRATEGY = "xlearning.input.strategy";

  public static final String DEFAULT_XLEARNING_INPUT_STRATEGY = "DOWNLOAD";

  public static final String XLEARNING_OUTPUT_STRATEGY = "xlearning.output.strategy";

  public static final String DEFAULT_XLEARNING_OUTPUT_STRATEGY = "UPLOAD";

  public static final String XLEARNING_STREAM_EPOCH = "xlearning.stream.epoch";

  public static final int DEFAULT_XLEARNING_STREAM_EPOCH = 1;

  public static final String XLEARNING_INPUT_STREAM_SHUFFLE = "xlearning.input.stream.shuffle";

  public static final Boolean DEFAULT_XLEARNING_INPUT_STREAM_SHUFFLE = false;

  public static final String XLEARNING_INPUTFORMAT_CACHESIZE_LIMIT= "xlearning.inputformat.cachesize.limit";

  public static final int DEFAULT_XLEARNING_INPUTFORMAT_CACHESIZE_LIMIT = 100 * 1024;

  public static final String XLEARNING_INPUTFORMAT_CACHE = "xlearning.inputformat.cache";

  public static final boolean DEFAULT_XLEARNING_INPUTFORMAT_CACHE = false;

  public static final String XLEARNING_INPUTFORMAT_CACHEFILE_NAME = "xlearning.inputformat.cachefile.name";

  public static final String DEFAULT_XLEARNING_INPUTFORMAT_CACHEFILE_NAME = "inputformatCache.gz";

  public static final String XLEARNING_INTERREAULST_DIR = "xlearning.interresult.dir";

  public static final String DEFAULT_XLEARNING_INTERRESULT_DIR = "/interResult_";

  public static final String XLEARNING_INTERRESULT_SAVE_INC = "xlearning.interresult.save.inc";

  public static final Boolean DEFAULT_XLEARNING_INTERRESULT_SAVE_INC = false;


  public static final String[] DEFAULT_XLEARNING_APPLICATION_CLASSPATH = {
      "$HADOOP_CONF_DIR",
      "$HADOOP_COMMON_HOME/share/hadoop/common/*",
      "$HADOOP_COMMON_HOME/share/hadoop/common/lib/*",
      "$HADOOP_HDFS_HOME/share/hadoop/hdfs/*",
      "$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*",
      "$HADOOP_YARN_HOME/share/hadoop/yarn/*",
      "$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*",
      "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*",
      "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*"
  };

  public static final String XLEARNING_TF_DISTRIBUTION_STRATEGY = "xlearning.tf.distribution.strategy";
  public static final Boolean DEFAULT_XLEARNING_TF_DISTRIBUTION_STRATEGY = false;
  public static final String XLEARNING_TF_BOARD_PATH = "xlearning.tf.board.path";
  public static final String DEFAULT_XLEARNING_TF_BOARD_PATH = "tensorboard";
  public static final String XLEARNING_TF_BOARD_WORKER_INDEX = "xlearning.tf.board.worker.index";
  public static final int DEFAULT_XLEARNING_TF_BOARD_WORKER_INDEX = 0;
  public static final String XLEARNING_TF_BOARD_RELOAD_INTERVAL = "xlearning.tf.board.reload.interval";
  public static final int DEFAULT_XLEARNING_TF_BOARD_RELOAD_INTERVAL = 1;
  public static final String XLEARNING_TF_BOARD_LOG_DIR = "xlearning.tf.board.log.dir";
  public static final String DEFAULT_XLEARNING_TF_BOARD_LOG_DIR = "eventLog";
  public static final String XLEARNING_TF_BOARD_ENABLE = "xlearning.tf.board.enable";
  public static final Boolean DEFAULT_XLEARNING_TF_BOARD_ENABLE = true;
  public static final String XLEARNING_TF_BOARD_HISTORY_DIR = "xlearning.tf.board.history.dir";
  public static final String DEFAULT_XLEARNING_TF_BOARD_HISTORY_DIR = "/tmp/XLearning/eventLog";
  public static final String XLEARNING_BOARD_PATH = "xlearning.board.path";
  public static final String DEFAULT_XLEARNING_BOARD_PATH = "visualDL";
  public static final String XLEARNING_BOARD_MODELPB = "xlearning.board.modelpb";
  public static final String DEFAULT_XLEARNING_BOARD_MODELPB = "";
  public static final String XLEARNING_BOARD_CACHE_TIMEOUT = "xlearning.board.cache.timeout";
  public static final int DEFAULT_XLEARNING_BOARD_CACHE_TIMEOUT = 20;
  /**
   * Configuration used in ApplicationMaster
   */
  public static final String XLEARNING_CONTAINER_EXTRA_JAVA_OPTS = "xlearning.container.extra.java.opts";

  public static final String DEFAULT_XLEARNING_CONTAINER_JAVA_OPTS_EXCEPT_MEMORY = "";

  public static final String XLEARNING_ALLOCATE_INTERVAL = "xlearning.allocate.interval";

  public static final int DEFAULT_XLEARNING_ALLOCATE_INTERVAL = 1000;

  public static final String XLEARNING_STATUS_UPDATE_INTERVAL = "xlearning.status.update.interval";

  public static final int DEFAULT_XLEARNING_STATUS_PULL_INTERVAL = 1000;

  public static final String XLEARNING_TASK_TIMEOUT = "xlearning.task.timeout";

  public static final int DEFAULT_XLEARNING_TASK_TIMEOUT = 5 * 60 * 1000;

  public static final String XLEARNING_LOCALRESOURCE_TIMEOUT = "xlearning.localresource.timeout";

  public static final int DEFAULT_XLEARNING_LOCALRESOURCE_TIMEOUT = 5 * 60 * 1000;

  public static final String XLEARNING_TASK_TIMEOUT_CHECK_INTERVAL_MS = "xlearning.task.timeout.check.interval";

  public static final int DEFAULT_XLEARNING_TASK_TIMEOUT_CHECK_INTERVAL_MS = 3 * 1000;

  public static final String XLEARNING_INTERRESULT_UPLOAD_TIMEOUT = "xlearning.interresult.upload.timeout";

  public static final int DEFAULT_XLEARNING_INTERRESULT_UPLOAD_TIMEOUT = 50 * 60 * 1000;

  public static final String XLEARNING_MESSAGES_LEN_MAX = "xlearning.messages.len.max";

  public static final int DEFAULT_XLEARNING_MESSAGES_LEN_MAX = 1000;

  public static final String XLEARNING_EXECUTE_NODE_LIMIT = "xlearning.execute.node.limit";

  public static final int DEFAULT_XLEARNING_EXECUTENODE_LIMIT = 200;

  public static final String XLEARNING_CLEANUP_ENABLE = "xlearning.cleanup.enable";

  public static final boolean DEFAULT_XLEARNING_CLEANUP_ENABLE = true;

  public static final String XLEARNING_CONTAINER_MAX_FAILURES_RATE = "xlearning.container.maxFailures.rate";

  public static final double DEFAULT_XLEARNING_CONTAINER_FAILURES_RATE = 0.5;

  public static final String XLEARNING_ENV_MAXLENGTH = "xlearning.env.maxlength";

  public static final Integer DEFAULT_XLEARNING_ENV_MAXLENGTH = 102400;

  /**
   * Configuration used in Container
   */
  public static final String XLEARNING_DOWNLOAD_FILE_RETRY = "xlearning.download.file.retry";

  public static final int DEFAULT_XLEARNING_DOWNLOAD_FILE_RETRY = 3;

  public static final String XLEARNING_DOWNLOAD_FILE_THREAD_NUMS = "xlearning.download.file.thread.nums";

  public static final int DEFAULT_XLEARNING_DOWNLOAD_FILE_THREAD_NUMS = 10;

  public static final String XLEARNING_UPLOAD_OUTPUT_THREAD_NUMS = "xlearning.upload.output.thread.nums";

  public static final String XLEARNING_FILE_LIST_LEVEL = "xlearning.file.list.level";

  public static final int DEFAULT_XLEARNING_FILE_LIST_LEVEL = 2;

  public static final String XLEARNING_CONTAINER_HEARTBEAT_INTERVAL = "xlearning.container.heartbeat.interval";

  public static final int DEFAULT_XLEARNING_CONTAINER_HEARTBEAT_INTERVAL = 10 * 1000;

  public static final String XLEARNING_CONTAINER_HEARTBEAT_RETRY = "xlearning.container.heartbeat.retry";

  public static final int DEFAULT_XLEARNING_CONTAINER_HEARTBEAT_RETRY = 3;

  public static final String XLEARNING_CONTAINER_UPDATE_APP_STATUS_INTERVAL = "xlearning.container.update.appstatus.interval";

  public static final int DEFAULT_XLEARNING_CONTAINER_UPDATE_APP_STATUS_INTERVAL = 3 * 1000;

  public static final String XLEARNING_CONTAINER_AUTO_CREATE_OUTPUT_DIR = "xlearning.container.auto.create.output.dir";

  public static final boolean DEFAULT_XLEARNING_CONTAINER_AUTO_CREATE_OUTPUT_DIR = true;

  public static final String XLEARNING_RESERVE_PORT_BEGIN = "xlearning.reserve.port.begin";

  public static final int DEFAULT_XLEARNING_RESERVE_PORT_BEGIN = 20000;

  public static final String XLEARNING_RESERVE_PORT_END = "xlearning.reserve.port.end";

  public static final int DEFAULT_XLEARNING_RESERVE_PORT_END = 30000;

  /**
   * Configuration used in Log Dir
   */
  public static final String XLEARNING_HISTORY_LOG_DIR = "xlearning.history.log.dir";

  public static final String DEFAULT_XLEARNING_HISTORY_LOG_DIR = "/tmp/XLearning/history";

  public static final String XLEARNING_HISTORY_LOG_DELETE_MONITOR_TIME_INTERVAL = "xlearning.history.log.delete-monitor-time-interval";

  public static final int DEFAULT_XLEARNING_HISTORY_LOG_DELETE_MONITOR_TIME_INTERVAL = 24 * 60 * 60 * 1000;

  public static final String XLEARNING_HISTORY_LOG_MAX_AGE_MS = "xlearning.history.log.max-age-ms";

  public static final int DEFAULT_XLEARNING_HISTORY_LOG_MAX_AGE_MS = 24 * 60 * 60 * 1000;

  /**
   * Configuration used in Job History
   */
  public static final String XLEARNING_HISTORY_ADDRESS = "xlearning.history.address";

  public static final String XLEARNING_HISTORY_PORT = "xlearning.history.port";

  public static final int DEFAULT_XLEARNING_HISTORY_PORT = 10021;

  public static final String DEFAULT_XLEARNING_HISTORY_ADDRESS = "0.0.0.0:" + DEFAULT_XLEARNING_HISTORY_PORT;

  public static final String XLEARNING_HISTORY_WEBAPP_ADDRESS = "xlearning.history.webapp.address";

  public static final String XLEARNING_HISTORY_WEBAPP_PORT = "xlearning.history.webapp.port";

  public static final int DEFAULT_XLEARNING_HISTORY_WEBAPP_PORT = 19886;

  public static final String DEFAULT_XLEARNING_HISTORY_WEBAPP_ADDRESS = "0.0.0.0:" + DEFAULT_XLEARNING_HISTORY_WEBAPP_PORT;

  public static final String XLEARNING_HISTORY_WEBAPP_HTTPS_ADDRESS = "xlearning.history.webapp.https.address";

  public static final String XLEARNING_HISTORY_WEBAPP_HTTPS_PORT = "xlearning.history.webapp.https.port";

  public static final int DEFAULT_XLEARNING_HISTORY_WEBAPP_HTTPS_PORT = 19885;

  public static final String DEFAULT_XLEARNING_HISTORY_WEBAPP_HTTPS_ADDRESS = "0.0.0.0:" + DEFAULT_XLEARNING_HISTORY_WEBAPP_HTTPS_PORT;

  public static final String XLEARNING_HISTORY_BIND_HOST = "xlearning.history.bind-host";

  public static final String XLEARNING_HISTORY_CLIENT_THREAD_COUNT = "xlearning.history.client.thread-count";

  public static final int DEFAULT_XLEARNING_HISTORY_CLIENT_THREAD_COUNT = 10;

  public static final String XLEARNING_HS_RECOVERY_ENABLE = "xlearning.history.recovery.enable";

  public static final boolean DEFAULT_XLEARNING_HS_RECOVERY_ENABLE = false;

  public static final String XLEARNING_HISTORY_KEYTAB = "xlearning.history.keytab";

  public static final String XLEARNING_HISTORY_PRINCIPAL = "xlearning.history.principal";

  public static final String XLEARNING_AM_ENV = "yarn.app.xlearning.am.env";
  public static final String DEFAULT_XLEARNING_AM_ENV = "";

  public static final String XLEARNING_CONTAINER_ENV = "yarn.app.xlearning.container.env";
  public static final String DEFAULT_XLEARNING_CONTAINER_ENV = "";

  /**
   * To enable https in XLEARNING history server
   */
  public static final String XLEARNING_HS_HTTP_POLICY = "xlearning.history.http.policy";
  public static String DEFAULT_XLEARNING_HS_HTTP_POLICY =
      HttpConfig.Policy.HTTP_ONLY.name();

  /**
   * The kerberos principal to be used for spnego filter for history server
   */
  public static final String XLEARNING_WEBAPP_SPNEGO_USER_NAME_KEY = "xlearning.webapp.spnego-principal";

  /**
   * The kerberos keytab to be used for spnego filter for history server
   */
  public static final String XLEARNING_WEBAPP_SPNEGO_KEYTAB_FILE_KEY = "xlearning.webapp.spnego-keytab-file";

  //Delegation token related keys
  public static final String DELEGATION_KEY_UPDATE_INTERVAL_KEY =
      "xlearning.cluster.delegation.key.update-interval";
  public static final long DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT =
      24 * 60 * 60 * 1000; // 1 day
  public static final String DELEGATION_TOKEN_RENEW_INTERVAL_KEY =
      "xlearning.cluster.delegation.token.renew-interval";
  public static final long DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT =
      24 * 60 * 60 * 1000;  // 1 day
  public static final String DELEGATION_TOKEN_MAX_LIFETIME_KEY =
      "xlearning.cluster.delegation.token.max-lifetime";
  public static final long DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT =
      24 * 60 * 60 * 1000; // 7 days

  /**
   * xlearning support docker conf
   */
  public static final String XLEARNING_DOCKER_REGISTRY_HOST = "xlearning.docker.registry.host";

  public static final String XLEARNING_DOCKER_REGISTRY_PORT = "xlearning.docker.registry.port";

  public static final String XLEARNING_DOCKER_IMAGE= "xlearning.docker.image";

  public static final String XLEARNING_CONTAINER_TYPE= "xlearning.container.type";
  public static final String DEFAULT_XLEARNING_CONTAINER_TYPE = "yarn";

  public static final String XLEARNING_DOCKER_RUN_ARGS = "xlearning.docker.run.args";

  public static final String XLEARNING_DOCKER_WORK_DIR = "xlearning.docker.worker.dir";

  public static final String DEFAULT_XLEARNING_DOCKER_WORK_DIR = "work";

  /** Configuration for mpi app */
  public static final String XLEARNING_MPI_EXTRA_LD_LIBRARY_PATH = "xlearning.mpi.extra.ld.library.path";

  public static final String XLEARNING_MPI_INSTALL_DIR = "xlearning.mpi.install.dir";
  public static final String DEFAULT_XLEARNING_MPI_INSTALL_DIR = "/usr/local/openmpi/";

  public static final String XLEARNING_MPI_CONTAINER_UPDATE_APP_STATUS_RETRY = "xlearning.mpi.container.update.status.retry";
  public static final int DEFAULT_XLEARNING_MPI_CONTAINER_UPDATE_APP_STATUS_RETRY = 3;

}
