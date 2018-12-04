package net.qihoo.hbox.AM;

import com.google.gson.Gson;
import net.qihoo.hbox.api.ApplicationContext;
import net.qihoo.hbox.api.HboxConstants;
import net.qihoo.hbox.common.*;
import net.qihoo.hbox.common.exceptions.HboxExecException;
import net.qihoo.hbox.conf.HboxConfiguration;
import net.qihoo.hbox.container.HboxContainerId;
import net.qihoo.hbox.util.Utilities;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.RoundingMode;
import java.net.*;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class ApplicationMaster extends CompositeService {

  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
  private final Configuration conf;
  private Map<String, String> envs;
  private AMRMClientAsync<ContainerRequest> amrmAsync;
  private NMClientAsync nmAsync;
  private ApplicationAttemptId applicationAttemptID;
  private String applicationMasterHostname;
  private String applicationMasterTrackingUrl;
  private String applicationHistoryUrl;
  private int workerMemory;
  private int workerVCores;
  private int workerGCores;
  private int workerNum;
  private int psMemory;
  private int psVCores;
  private int psGCores;
  private int psNum;
  private int maxContainerMem;
  private Boolean single;
  private Boolean singleMx;
  private int appPriority;
  // location of AppMaster.jar on HDFS
  private Path appJarRemoteLocation;
  // location of job.xml on HDFS
  private Path appConfRemoteLocation;
  // location of files on HDFS
  private String appFilesRemoteLocation;
  // location of lib jars on HDFS
  private String appLibJarsRemoteLocation;
  // location of cacheFiles on HDFS
  private String appCacheFilesRemoteLocation;
  // location of cacheArchive on HDFS
  private String appCacheArchivesRemoteLocation;
  private String hboxCommand;
  private String dmlcPsRootUri;
  private int dmlcPsRootPort;
  private String dmlcTrackerUri;
  private int dmlcTrackerPort;
  private String hboxAppType;
  private String userName;
  private List<Container> acquiredWorkerContainers;
  private List<Container> acquiredPsContainers;
  private final LinkedBlockingQueue<Message> applicationMessageQueue;
  private final List<OutputInfo> outputInfos;
  private ConcurrentHashMap<String, List<FileStatus>> input2FileStatus;
  private ConcurrentHashMap<HboxContainerId, List<InputInfo>> containerId2InputInfo;
  private ConcurrentHashMap<String, InputInfo> wholeFiles;
  private InputSplit[] inputFileSplits;
  private ConcurrentHashMap<HboxContainerId, List<InputSplit>> containerId2InputSplit;
  // An RPC Service listening the container status
  private ApplicationContainerListener containerListener;
  private int statusUpdateInterval;
  private final ApplicationContext applicationContext;
  private RMCallbackHandler rmCallbackHandler;
  private ContainerRequest workerContainerRequest;
  private ContainerRequest psContainerRequest;
  private Map<String, LocalResource> containerLocalResource;
  private ApplicationWebService webService;
  private ApplicationMessageService messageService;

  private Boolean startSavingModel;
  private Boolean lastSavingStatus;
  private List<Long> savingModelList;
  private int savingInterval;

  private Thread cleanApplication;
  private String libJarsClassPath;
  private Set<String> containerHostnames;
  private String[] hostLocals;

  private String mpiExecDir;
  private Process mpiExecProcess;
  private String mpiContainerCommand;
  private StringBuilder reLinkFiles;
  private int mpiExitCode;

  private Boolean tfEvaluator;
  private String tfEvaluatorContainerId;
  private StringBuilder inputPath;
  private List<String> inputList;

  private String amContainerId;
  private StringBuilder amContainerStdOut;
  private StringBuilder amContainerStdErr;

  private Boolean containerStarted;

  private int reservePortBegin = 0;
  private int reservePortEnd = 0;

  /**
   * Constructor, connect to Resource Manager
   *
   * @throws IOException
   */
  private ApplicationMaster() {
    super(ApplicationMaster.class.getName());

    conf = new HboxConfiguration();
    conf.addResource(new Path(HboxConstants.HBOX_JOB_CONFIGURATION));
    outputInfos = new ArrayList<>();
    input2FileStatus = new ConcurrentHashMap<>();
    containerId2InputInfo = new ConcurrentHashMap<>();
    wholeFiles = new ConcurrentHashMap<>();
    inputFileSplits = null;
    containerId2InputSplit = new ConcurrentHashMap<>();
    statusUpdateInterval = conf.getInt(HboxConfiguration.HBOX_STATUS_UPDATE_INTERVAL, HboxConfiguration.DEFAULT_HBOX_STATUS_PULL_INTERVAL);
    applicationAttemptID = Records.newRecord(ApplicationAttemptId.class);
    applicationMessageQueue = new LinkedBlockingQueue<>(
        conf.getInt(HboxConfiguration.HBOX_MESSAGES_LEN_MAX, HboxConfiguration.DEFAULT_HBOX_MESSAGES_LEN_MAX));
    containerLocalResource = new HashMap<>();
    applicationContext = new RunningAppContext();

    envs = System.getenv();
    maxContainerMem = Integer.valueOf(envs.get(HboxConstants.Environment.HBOX_CONTAINER_MAX_MEMORY.toString()));
    workerMemory = conf.getInt(HboxConfiguration.HBOX_WORKER_MEMORY, HboxConfiguration.DEFAULT_HBOX_WORKER_MEMORY);
    workerVCores = conf.getInt(HboxConfiguration.HBOX_WORKER_VCORES, HboxConfiguration.DEFAULT_HBOX_WORKER_VCORES);
    workerGCores = conf.getInt(HboxConfiguration.HBOX_WORKER_GPU, HboxConfiguration.DEFAULT_HBOX_WORKER_GPU);
    workerNum = conf.getInt(HboxConfiguration.HBOX_WORKER_NUM, HboxConfiguration.DEFAULT_HBOX_WORKER_NUM);
    psMemory = conf.getInt(HboxConfiguration.HBOX_PS_MEMORY, HboxConfiguration.DEFAULT_HBOX_PS_MEMORY);
    psVCores = conf.getInt(HboxConfiguration.HBOX_PS_VCORES, HboxConfiguration.DEFAULT_HBOX_PS_VCORES);
    psGCores = conf.getInt(HboxConfiguration.HBOX_PS_GPU, HboxConfiguration.DEFAULT_HBOX_PS_GPU);
    psNum = conf.getInt(HboxConfiguration.HBOX_PS_NUM, HboxConfiguration.DEFAULT_HBOX_PS_NUM);
    single = conf.getBoolean(HboxConfiguration.HBOX_TF_MODE_SINGLE, HboxConfiguration.DEFAULT_HBOX_TF_MODE_SINGLE);
    singleMx = conf.getBoolean(HboxConfiguration.HBOX_MXNET_MODE_SINGLE, HboxConfiguration.DEFAULT_HBOX_MXNET_MODE_SINGLE);
    appPriority = conf.getInt(HboxConfiguration.HBOX_APP_PRIORITY, HboxConfiguration.DEFAULT_HBOX_APP_PRIORITY);
    tfEvaluator = conf.getBoolean(HboxConfiguration.HBOX_TF_EVALUATOR, HboxConfiguration.DEFAULT_HBOX_TF_EVALUATOR);
    tfEvaluatorContainerId = "";
    acquiredWorkerContainers = new ArrayList<>();
    acquiredPsContainers = new ArrayList<>();
    dmlcPsRootUri = null;
    dmlcPsRootPort = 0;
    dmlcTrackerUri = null;
    dmlcTrackerPort = 0;
    libJarsClassPath = "";
    containerHostnames = null;
    hostLocals = null;
    reLinkFiles = new StringBuilder();
    inputPath = new StringBuilder();
    inputList = new ArrayList<>();

    amContainerStdOut = new StringBuilder();
    amContainerStdErr = new StringBuilder();
    containerStarted = false;

    reservePortBegin = this.conf.getInt(HboxConfiguration.HBOX_RESERVE_PORT_BEGIN, HboxConfiguration.DEFAULT_HBOX_RESERVE_PORT_BEGIN);
    reservePortEnd = this.conf.getInt(HboxConfiguration.HBOX_RESERVE_PORT_END, HboxConfiguration.DEFAULT_HBOX_RESERVE_PORT_END);

    if (envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.toString())) {
      amContainerId = envs.get(ApplicationConstants.Environment.CONTAINER_ID.toString());
      ContainerId containerId = ConverterUtils
          .toContainerId(amContainerId);
      applicationAttemptID = containerId.getApplicationAttemptId();
    } else {
      throw new IllegalArgumentException(
          "Application Attempt Id is not availiable in environment");
    }

    LOG.info("Application appId="
        + applicationAttemptID.getApplicationId().getId()
        + ", clustertimestamp="
        + applicationAttemptID.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + applicationAttemptID.getAttemptId());

    if (applicationAttemptID.getAttemptId() > 1 && (conf.getInt(HboxConfiguration.HBOX_APP_MAX_ATTEMPTS, HboxConfiguration.DEFAULT_HBOX_APP_MAX_ATTEMPTS) > 1)) {
      workerMemory = workerMemory + (applicationAttemptID.getAttemptId() - 1) * (int) Math.ceil(workerMemory * conf.getDouble(HboxConfiguration.HBOX_WORKER_MEM_AUTO_SCALE, HboxConfiguration.DEFAULT_HBOX_WORKER_MEM_AUTO_SCALE));
      if (workerMemory > maxContainerMem) {
        workerMemory = maxContainerMem;
      }
      LOG.info("Auto Scale the Worker Memory from " + conf.getInt(HboxConfiguration.HBOX_WORKER_MEMORY, HboxConfiguration.DEFAULT_HBOX_WORKER_MEMORY) + " to " + workerMemory);
      if (psNum > 0) {
        psMemory = psMemory + (applicationAttemptID.getAttemptId() - 1) * (int) Math.ceil(psMemory * conf.getDouble(HboxConfiguration.HBOX_PS_MEM_AUTO_SCALE, HboxConfiguration.DEFAULT_HBOX_PS_MEM_AUTO_SCALE));
        if (psMemory > maxContainerMem) {
          psMemory = maxContainerMem;
        }
        LOG.info("Auto Scale the Ps Memory from " + conf.getInt(HboxConfiguration.HBOX_PS_MEMORY, HboxConfiguration.DEFAULT_HBOX_PS_MEMORY) + " to " + psMemory);
      }
    }

    if (envs.containsKey(HboxConstants.Environment.HBOX_FILES_LOCATION.toString())) {
      appFilesRemoteLocation = envs.get(HboxConstants.Environment.HBOX_FILES_LOCATION.toString());
      LOG.info("Application files location: " + appFilesRemoteLocation);
    }

    if (envs.containsKey(HboxConstants.Environment.HBOX_LIBJARS_LOCATION.toString())) {
      appLibJarsRemoteLocation = envs.get(HboxConstants.Environment.HBOX_LIBJARS_LOCATION.toString());
      LOG.info("Application lib Jars location: " + appLibJarsRemoteLocation);
    }

    if (envs.containsKey(HboxConstants.Environment.HBOX_CACHE_FILE_LOCATION.toString())) {
      appCacheFilesRemoteLocation = envs.get(HboxConstants.Environment.HBOX_CACHE_FILE_LOCATION.toString());
      LOG.info("Application cacheFiles location: " + appCacheFilesRemoteLocation);
    }

    if (envs.containsKey(HboxConstants.Environment.HBOX_CACHE_ARCHIVE_LOCATION.toString())) {
      appCacheArchivesRemoteLocation = envs.get(HboxConstants.Environment.HBOX_CACHE_ARCHIVE_LOCATION.toString());
      LOG.info("Application cacheArchive location: " + appCacheArchivesRemoteLocation);
    }

    assert (envs.containsKey(HboxConstants.Environment.APP_JAR_LOCATION.toString()));
    appJarRemoteLocation = new Path(envs.get(HboxConstants.Environment.APP_JAR_LOCATION.toString()));
    LOG.info("Application jar location: " + appJarRemoteLocation);

    assert (envs.containsKey(HboxConstants.Environment.HBOX_JOB_CONF_LOCATION.toString()));
    appConfRemoteLocation = new Path(envs.get(HboxConstants.Environment.HBOX_JOB_CONF_LOCATION.toString()));
    LOG.info("Application conf location: " + appConfRemoteLocation);

    if (envs.containsKey(HboxConstants.Environment.HBOX_EXEC_CMD.toString())) {
      hboxCommand = envs.get(HboxConstants.Environment.HBOX_EXEC_CMD.toString());
      LOG.info("Hbox exec command: " + hboxCommand);
    }

    if (envs.containsKey(HboxConstants.Environment.HBOX_APP_TYPE.toString())) {
      hboxAppType = envs.get(HboxConstants.Environment.HBOX_APP_TYPE.toString()).toUpperCase();
      LOG.info("Hbox app type: " + hboxAppType);
    } else {
      hboxAppType = HboxConfiguration.DEFAULT_HBOX_APP_TYPE.toUpperCase();
      LOG.info("Hbox app type: " + hboxAppType);
    }

    if(hboxAppType.equals("MPI") || hboxAppType.equals("HOROVOD")) {
      Path pwd = new Path(envs.get("PWD"));
      mpiExecDir = pwd.getParent().toString();
      if (conf.getBoolean(HboxConfiguration.HBOX_MPI_EXEC_DIR_ENABLE, HboxConfiguration.DEFAULT_HBOX_MPI_EXEC_DIR_ENABLE)) {
        mpiExecDir = conf.get(HboxConfiguration.HBOX_MPI_EXEC_DIR, HboxConfiguration.DEFAULT_HBOX_MPI_EXEC_DIR);
      }
      LOG.info(hboxAppType + " exec path: " + mpiExecDir);
    }

    if (envs.containsKey(ApplicationConstants.Environment.NM_HOST.toString())) {
      applicationMasterHostname = envs.get(ApplicationConstants.Environment.NM_HOST.toString());
    }

    this.messageService = new ApplicationMessageService(this.applicationContext, conf);
    this.webService = new ApplicationWebService(this.applicationContext, conf);
    this.containerListener = new ApplicationContainerListener(applicationContext, conf);

    this.startSavingModel = false;
    this.lastSavingStatus = false;
    this.savingModelList = new ArrayList<>();
    this.savingInterval = conf.getInt(HboxConfiguration.HBOX_INTERRESULT_SAVE_INTERVAL, HboxConfiguration.DEFAULT_HBOX_INTERRESULT_SAVE_INTERVAL);
  }

  private void init() {
    appendMessage(new Message(LogType.STDERR, "ApplicationMaster starting services"));

    this.rmCallbackHandler = new RMCallbackHandler();
    this.amrmAsync = AMRMClientAsync.createAMRMClientAsync(1000, rmCallbackHandler);
    this.amrmAsync.init(conf);

    NMCallbackHandler nmAsyncHandler = new NMCallbackHandler();
    this.nmAsync = NMClientAsync.createNMClientAsync(nmAsyncHandler);
    this.nmAsync.init(conf);

    addService(this.amrmAsync);
    addService(this.nmAsync);
    addService(this.messageService);
    addService(this.webService);
    addService(this.containerListener);
    try {
      super.serviceStart();
    } catch (Exception e) {
      throw new RuntimeException("Error start application services!", e);
    }

    applicationMasterTrackingUrl = applicationMasterHostname + ":" + this.webService.getHttpPort();
    String historyWebappAddress = conf.get(HboxConfiguration.HBOX_HISTORY_WEBAPP_ADDRESS,
        HboxConfiguration.DEFAULT_HBOX_HISTORY_WEBAPP_ADDRESS);
    String cluster = conf.get(HboxConfiguration.HBOX_CLUSTER_NAME, HboxConfiguration.DEFAULT_HBOX_CLUSTER_NAME);
    if (!(cluster.equals("") || cluster == null)) {
      String clusterHistoryWebappAddress = conf.get(HboxConfiguration.HBOX_CLUSTER_HISTORY_WEBAPP_ADDRESS.replace("cluster.name", cluster));
      if (clusterHistoryWebappAddress == null || clusterHistoryWebappAddress.equals("")) {
        LOG.warn("Note that not set the cluster history webaddress.");
      } else {
        historyWebappAddress = clusterHistoryWebappAddress;
        LOG.info("History webApp address has updated! ");
      }
    }
    applicationHistoryUrl = historyWebappAddress + "/jobhistory/job/"
        + applicationAttemptID.getApplicationId();
    LOG.info("master tracking url:" + applicationMasterTrackingUrl);
    LOG.info("history url: " + applicationHistoryUrl);

    cleanApplication = new Thread(new Runnable() {
      @Override
      public void run() {
        HboxConfiguration hboxConf = new HboxConfiguration();
        hboxConf.setBoolean("fs.hdfs.impl.disable.cache", true);
        hboxConf.setBoolean("fs.hdfsold.impl.disable.cache", true);

        if (hboxConf.getBoolean(HboxConfiguration.HBOX_CLEANUP_ENABLE, HboxConfiguration.DEFAULT_HBOX_CLEANUP_ENABLE)) {
          Path stagingDir = new Path(envs.get(HboxConstants.Environment.HBOX_STAGING_LOCATION.toString()));
          try {
            FileSystem stagingFS = FileSystem.get(hboxConf);
            stagingFS.delete(stagingDir);
            LOG.info("Deleting the staging file successed.");
          } catch (Exception e){
            LOG.info("Deleting the staging file Error." + e);
          }
        }

        try {
          FsPermission LOG_FILE_PERMISSION = FsPermission.createImmutable((short) 0777);
          Path logdir = new Path(conf.get(HboxConfiguration.HBOX_HISTORY_LOG_DIR,
              HboxConfiguration.DEFAULT_HBOX_HISTORY_LOG_DIR) + "/" + applicationAttemptID.getApplicationId().toString()
              + "/" + applicationAttemptID.getApplicationId().toString());
          Path jobLogPath = new Path(hboxConf.get("fs.defaultFS"), logdir);
          LOG.info("jobLogPath:" + jobLogPath.toString());
          LOG.info("Start write the log to " + jobLogPath.toString());
          FileSystem fs = FileSystem.get(hboxConf);
          FSDataOutputStream out = fs.create(jobLogPath);
          fs.setPermission(jobLogPath, new FsPermission(LOG_FILE_PERMISSION));
          if (conf.getBoolean(HboxConfiguration.HBOX_HOST_LOCAL_ENABLE, HboxConfiguration.DEFAULT_HBOX_HOST_LOCAL_ENABLE)) {
            Path hostLocaldir = new Path(conf.get(HboxConfiguration.HBOX_HISTORY_LOG_DIR,
                HboxConfiguration.DEFAULT_HBOX_HISTORY_LOG_DIR) + "/" + conf.get("hadoop.job.ugi").split(",")[0]
                + "/" + envs.get(HboxConstants.Environment.HBOX_APP_NAME.toString()));
            Path hostLocalPath = new Path(hboxConf.get("fs.defaultFS"), hostLocaldir);
            try {
              FSDataOutputStream hostLocalOut = fs.create(hostLocalPath);
              fs.setPermission(hostLocalPath, new FsPermission(LOG_FILE_PERMISSION));
              hostLocalOut.writeBytes(containerHostnames.toString().substring(1, containerHostnames.toString().length()-1));
              hostLocalOut.close();
              LOG.info("host local enable is true, write " + hostLocalPath.toString() + " success");
            } catch (Exception e) {
              LOG.info("write host local file error, " + e);
            }
          }
          Map<String, List<String>> logMessage = new HashMap<>();
          logMessage.put("appType", Arrays.asList(hboxAppType));
          if(!(hboxAppType.equals("VPC") || hboxAppType.equals("DIGITS"))) {
            List<String> tensorboardInfo = new ArrayList<>();
            if (conf.getBoolean(HboxConfiguration.HBOX_TF_BOARD_ENABLE, HboxConfiguration.DEFAULT_HBOX_TF_BOARD_ENABLE)) {
              Path boardLogPath;
              if (conf.get(HboxConfiguration.HBOX_TF_BOARD_LOG_DIR, HboxConfiguration.DEFAULT_HBOX_TF_BOARD_LOG_DIR).indexOf("hdfs://") == -1) {
                if (conf.get(HboxConfiguration.HBOX_TF_BOARD_HISTORY_DIR, HboxConfiguration.HBOX_TF_BOARD_HISTORY_DIR).equals(hboxConf.get(HboxConfiguration.HBOX_TF_BOARD_HISTORY_DIR, HboxConfiguration.HBOX_TF_BOARD_HISTORY_DIR))) {
                  boardLogPath = new Path(hboxConf.get("fs.defaultFS"), conf.get(HboxConfiguration.HBOX_TF_BOARD_HISTORY_DIR,
                      HboxConfiguration.DEFAULT_HBOX_TF_BOARD_HISTORY_DIR) + "/" + applicationAttemptID.getApplicationId().toString());
                } else {
                  boardLogPath = new Path(conf.get("fs.defaultFS"), conf.get(HboxConfiguration.HBOX_TF_BOARD_HISTORY_DIR,
                      HboxConfiguration.DEFAULT_HBOX_TF_BOARD_HISTORY_DIR));
                }
              } else {
                boardLogPath = new Path(conf.get(HboxConfiguration.HBOX_TF_BOARD_LOG_DIR));
              }
              tensorboardInfo.add(boardLogPath.toString());
            } else {
              tensorboardInfo.add("-");
            }
            logMessage.put("board", tensorboardInfo);
          }

          userName = StringUtils.split(conf.get("hadoop.job.ugi"), ',')[0];
          List<Container> workerContainers = applicationContext.getWorkerContainers();
          List<Container> psContainers = applicationContext.getPsContainers();
          Map<HboxContainerId, String> reporterProgress = applicationContext.getReporterProgress();
          Map<HboxContainerId, String> containersAppStartTime = applicationContext.getContainersAppStartTime();
          Map<HboxContainerId, String> containersAppFinishTime = applicationContext.getContainersAppFinishTime();

          for (Container container : workerContainers) {
            List<String> containerMessage = new ArrayList<>();
            containerMessage.add(container.getNodeHttpAddress());
            HboxContainerId currentContainerID = new HboxContainerId(container.getId());
            if (applicationContext.getContainerGPUDevice(currentContainerID) != null) {
              if (applicationContext.getContainerGPUDevice(currentContainerID).trim().length() != 0) {
                containerMessage.add(applicationContext.getContainerGPUDevice(currentContainerID));
              } else {
                containerMessage.add("-");
              }
            } else {
              containerMessage.add("-");
            }
            if (tfEvaluator && currentContainerID.toString().equals(tfEvaluatorContainerId)) {
              containerMessage.add(HboxConstants.EVALUATOR);
            } else {
              containerMessage.add(HboxConstants.WORKER);
            }
            if (applicationContext.getContainerStatus(currentContainerID) != null) {
              containerMessage.add(applicationContext.getContainerStatus(currentContainerID).toString());
            } else {
              containerMessage.add("-");
            }
            List<String> usageStatistics = new ArrayList<>();
            ConcurrentHashMap<String, LinkedBlockingDeque<Object>> cpuMetrics = applicationContext.getContainersCpuMetrics().get(currentContainerID);
            containerMessage.add(new Gson().toJson(cpuMetrics));
            ConcurrentHashMap<String, List<Double>> cpuStatistics = applicationContext.getContainersCpuStatistics().get(currentContainerID);
            usageStatistics.add(new Gson().toJson(cpuStatistics));
            if(workerGCores > 0) {
              ConcurrentHashMap<String, LinkedBlockingDeque<List<Long>>>  containersGpuMemMetrics = applicationContext.getContainersGpuMemMetrics().get(currentContainerID);
              ConcurrentHashMap<String, LinkedBlockingDeque<List<Long>>>  containersGpuUtilMetrics = applicationContext.getContainersGpuUtilMetrics().get(currentContainerID);
              containerMessage.add(new Gson().toJson(containersGpuMemMetrics));
              containerMessage.add(new Gson().toJson(containersGpuUtilMetrics));
              ConcurrentHashMap<String, List<Double>> gpuMemStatistics = applicationContext.getContainersGpuMemStatistics().get(currentContainerID);
              ConcurrentHashMap<String, List<Double>> gpuUtilStatistics = applicationContext.getContainersGpuUtilStatistics().get(currentContainerID);
              usageStatistics.add(new Gson().toJson(gpuMemStatistics));
              usageStatistics.add(new Gson().toJson(gpuUtilStatistics));
            } else {
              containerMessage.add("-");
              containerMessage.add("-");
              usageStatistics.add("-");
              usageStatistics.add("-");
            }

            if (cpuStatistics.size() != 0) {
              Double cpuMemUsagedMax = cpuStatistics.get("CPUMEM").get(1);
              if ((cpuMemUsagedMax / (workerMemory / 1024.0)) < conf.getDouble(HboxConfiguration.HBOX_CONTAINER_MEM_USAGE_WARN_FRACTION, HboxConfiguration.DEFAULT_HBOX_CONTAINER_MEM_USAGE_WARN_FRACTION)) {
                usageStatistics.add("true");
              } else {
                usageStatistics.add("false");
              }
            }

            if(containersAppStartTime.get(currentContainerID) != null && !containersAppStartTime.get(currentContainerID).equals("")) {
              String localStartTime = containersAppStartTime.get(currentContainerID);
              containerMessage.add(localStartTime);
            } else {
              containerMessage.add("N/A");
            }
            if(containersAppFinishTime.get(currentContainerID) != null && !containersAppFinishTime.get(currentContainerID).equals("")) {
              String localFinishTime = containersAppFinishTime.get(currentContainerID);
              containerMessage.add(localFinishTime);
            } else {
              containerMessage.add("N/A");
            }
            if(reporterProgress.get(currentContainerID) != null && !reporterProgress.get(currentContainerID).equals("")) {
              String progressLog = reporterProgress.get(currentContainerID);
              String[] progress = progressLog.toString().split(":");
              if(progress.length != 2) {
                containerMessage.add("progress log format error");
              } else {
                try {
                  Float percentProgress = Float.parseFloat(progress[1]);
                  if(percentProgress < 0.0 || percentProgress > 1.0) {
                    containerMessage.add("progress log format error");
                  } else {
                    DecimalFormat df = new DecimalFormat("0.00");
                    df.setRoundingMode(RoundingMode.HALF_UP);
                    containerMessage.add(df.format((Float.parseFloat(progress[1])*100)) + "%");
                    //containerMessage.add(Float.toString((Float.parseFloat(progress[1])*100)) + "%");
                  }
                } catch (Exception e) {
                  containerMessage.add("progress log format error");
                }
              }
              //containerMessage.add(Float.toString((Float.parseFloat(progress[1])*100)) + "%");
            } else {
              containerMessage.add("0.00%");
            }
            containerMessage.add(String.format("http://%s/node/containerlogs/%s/%s",
                container.getNodeHttpAddress(),
                container.getId().toString(),
                userName));
            containerMessage.addAll(usageStatistics);
            logMessage.put(container.getId().toString(), containerMessage);
          }

          for (Container container : psContainers) {
            List<String> containerMessage = new ArrayList<>();
            containerMessage.add(container.getNodeHttpAddress());
            HboxContainerId currentContainerID = new HboxContainerId(container.getId());
            if (applicationContext.getContainerGPUDevice(currentContainerID) != null) {
              if (applicationContext.getContainerGPUDevice(currentContainerID).trim().length() != 0) {
                containerMessage.add(applicationContext.getContainerGPUDevice(currentContainerID));
              } else {
                containerMessage.add("-");
              }
            } else {
              containerMessage.add("-");
            }
            if(hboxAppType.equals("TENSORFLOW")) {
              containerMessage.add("ps");
            } else if (hboxAppType.equals("MXNET") || hboxAppType.equals("DISTLIGHTLDA") || hboxAppType.equals("XFLOW")) {
              containerMessage.add("server");
            }

            if (applicationContext.getContainerStatus(currentContainerID) != null) {
              containerMessage.add(applicationContext.getContainerStatus(currentContainerID).toString());
            } else {
              containerMessage.add("-");
            }
            List<String> usageStatistics = new ArrayList<>();
            ConcurrentHashMap<String, LinkedBlockingDeque<Object>> cpuMetrics = applicationContext.getContainersCpuMetrics().get(currentContainerID);
            containerMessage.add(new Gson().toJson(cpuMetrics));
            ConcurrentHashMap<String, List<Double>> cpuStatistics = applicationContext.getContainersCpuStatistics().get(currentContainerID);
            usageStatistics.add(new Gson().toJson(cpuStatistics));

            if (psGCores > 0) {
              ConcurrentHashMap<String, LinkedBlockingDeque<List<Long>>> containersGpuMemMetrics = applicationContext.getContainersGpuMemMetrics().get(currentContainerID);
              ConcurrentHashMap<String, LinkedBlockingDeque<List<Long>>> containersGpuUtilMetrics = applicationContext.getContainersGpuUtilMetrics().get(currentContainerID);
              containerMessage.add(new Gson().toJson(containersGpuMemMetrics));
              containerMessage.add(new Gson().toJson(containersGpuUtilMetrics));
              ConcurrentHashMap<String, List<Double>> gpuMemStatistics = applicationContext.getContainersGpuMemStatistics().get(currentContainerID);
              ConcurrentHashMap<String, List<Double>> gpuUtilStatistics = applicationContext.getContainersGpuUtilStatistics().get(currentContainerID);
              usageStatistics.add(new Gson().toJson(gpuMemStatistics));
              usageStatistics.add(new Gson().toJson(gpuUtilStatistics));
            } else {
              containerMessage.add("-");
              containerMessage.add("-");
              usageStatistics.add("-");
              usageStatistics.add("-");
            }

            if (cpuStatistics.size() != 0) {
              Double cpuMemUsagedMax = cpuStatistics.get("CPUMEM").get(1);
              if ((cpuMemUsagedMax / (psMemory / 1024.0)) < conf.getDouble(HboxConfiguration.HBOX_CONTAINER_MEM_USAGE_WARN_FRACTION, HboxConfiguration.DEFAULT_HBOX_CONTAINER_MEM_USAGE_WARN_FRACTION)) {
                usageStatistics.add("true");
              } else {
                usageStatistics.add("false");
              }
            }

            if(containersAppStartTime.get(currentContainerID) != null && !containersAppStartTime.get(currentContainerID).equals("")) {
              String localStartTime = containersAppStartTime.get(currentContainerID);
              containerMessage.add(localStartTime);
            } else {
              containerMessage.add("N/A");
            }
            if(containersAppFinishTime.get(currentContainerID) != null && !containersAppFinishTime.get(currentContainerID).equals("")) {
              String localFinishTime = containersAppFinishTime.get(currentContainerID);
              containerMessage.add(localFinishTime);
            } else {
              containerMessage.add("N/A");
            }
            containerMessage.add("0.00%");
            containerMessage.add(String.format("http://%s/node/containerlogs/%s/%s",
                container.getNodeHttpAddress(),
                container.getId().toString(),
                userName));
            containerMessage.addAll(usageStatistics);
            logMessage.put(container.getId().toString(), containerMessage);
          }

          List<String> savedTimeStamp = new ArrayList<>();
          List<String> outputList = new ArrayList<>();
          if (applicationContext.getOutputs().size() == 0) {
            outputList.add("-");
            savedTimeStamp.add("-");
          } else {
            for (OutputInfo output : applicationContext.getOutputs()) {
              outputList.add(output.getDfsLocation());
            }
            if (applicationContext.getModelSavingList().size() == 0) {
              savedTimeStamp.add("-");
            } else {
              for (int i = applicationContext.getModelSavingList().size(); i > 0; i--) {
                savedTimeStamp.add(String.valueOf(applicationContext.getModelSavingList().get(i - 1)));
              }
            }
          }

          logMessage.put("input", inputList);
          logMessage.put("savedTimeStamp", savedTimeStamp);
          logMessage.put("output", outputList);
          logMessage.put("workerGcores", Arrays.asList(String.valueOf(workerGCores)));
          logMessage.put("workerNums", Arrays.asList(String.valueOf(workerNum)));
          logMessage.put("workerVCores", Arrays.asList(String.valueOf(workerVCores)));
          logMessage.put("workerMemory", Arrays.asList(String.format("%.2f", workerMemory / 1024.0)));
          logMessage.put("psGcores", Arrays.asList(String.valueOf(psGCores)));
          logMessage.put("psNums", Arrays.asList(String.valueOf(psNum)));
          logMessage.put("psVCores", Arrays.asList(String.valueOf(psVCores)));
          logMessage.put("psMemory", Arrays.asList(String.format("%.2f", psMemory / 1024.0)));
          logMessage.put("hboxVersion", Arrays.asList("1.3a"));
          logMessage.put("queue", Arrays.asList(conf.get(HboxConfiguration.HBOX_APP_QUEUE, HboxConfiguration.DEFAULT_HBOX_APP_QUEUE)));
          logMessage.put("user", Arrays.asList(conf.get("hadoop.job.ugi").split(",")[0]));

          out.writeBytes(new Gson().toJson(logMessage));
          out.close();
          fs.close();
          LOG.info("Writing the history log file Success.");
        } catch (Exception e) {
          LOG.info("Writing the history log file Error." + e);
        }
      }
    });
    Runtime.getRuntime().addShutdownHook(cleanApplication);
  }

  private void buildInputFileStatus() {
    String hboxInputs = envs.get(HboxConstants.Environment.HBOX_INPUTS.toString());
    if (StringUtils.isBlank(hboxInputs)) {
      LOG.info("Application has no inputs");
      return;
    }

    String[] inputs = StringUtils.split(hboxInputs, "|");
    if (inputs != null && inputs.length > 0) {
      for (String input : inputs) {
        String[] inputPathTuple = StringUtils.split(input, "#");
        if (inputPathTuple.length < 2) {
          throw new RuntimeException("Error input path format " + hboxInputs);
        }
        List<FileStatus> fileStatus = new ArrayList<>();
        String inputPathRemote = inputPathTuple[0];
        if (!StringUtils.isBlank(inputPathRemote)) {
          for (String singlePath : StringUtils.split(inputPathRemote, ",")) {
            Path inputPath = new Path(singlePath);
            this.inputList.add(singlePath);
            try {
              inputPath = inputPath.getFileSystem(conf).makeQualified(inputPath);

              List<FileStatus> downLoadFile = Utilities.listStatusRecursively(inputPath,
                  inputPath.getFileSystem(conf), null);
              fileStatus.addAll(downLoadFile);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
          input2FileStatus.put(inputPathTuple[1], fileStatus);
          this.inputPath.append(inputPathTuple[1]).append(",");
          if (fileStatus.size() > 0) {
            if (fileStatus.size() < workerNum) {
              LOG.warn("File count in  " + inputPathRemote + "  " + fileStatus.size() +
                  " less than the worker count " + workerNum );
            }
          }
        } else {
          throw new RuntimeException("Error input path format " + hboxInputs);
        }
      }
    }
  }

  public void buildInputStreamFileStatus() throws IOException {
    String hboxInputs = envs.get(HboxConstants.Environment.HBOX_INPUTS.toString());
    if (StringUtils.isBlank(hboxInputs)) {
      LOG.info("Application has no inputs");
      return;
    }

    String[] inputPathTuple = StringUtils.split(hboxInputs, "#");
    if (inputPathTuple.length < 2) {
      throw new RuntimeException("Error input path format " + hboxInputs);
    }
    String inputPathRemote = inputPathTuple[0];
    if (!StringUtils.isBlank(inputPathRemote)) {
      JobConf jobConf = new JobConf(conf);
      jobConf.set(HboxConstants.STREAM_INPUT_DIR, inputPathRemote);
      InputFormat inputFormat = ReflectionUtils.newInstance(conf.getClass(HboxConfiguration.HBOX_INPUTF0RMAT_CLASS, HboxConfiguration.DEFAULT_HBOX_INPUTF0RMAT_CLASS, InputFormat.class),
              jobConf);
      inputFileSplits = inputFormat.getSplits(jobConf, 1);
    } else {
      throw new RuntimeException("Error input path format " + hboxInputs);
    }
  }

  @SuppressWarnings("deprecation")
  private void allocateInputSplits() {

    for (Container container : acquiredWorkerContainers) {
      LOG.info("Initializing " + container.getId().toString() + " input splits");
      containerId2InputInfo.putIfAbsent(new HboxContainerId(container.getId()), new ArrayList<InputInfo>());
    }
    Set<String> fileKeys = input2FileStatus.keySet();
    int splitWorkerNum = workerNum;
    if (tfEvaluator) {
      --splitWorkerNum;
      LOG.info("Note that current tensorflow job has the evaluator type. Not allocate the input to the last container.");
    }
    for (String fileName : fileKeys) {
      List<FileStatus> files = input2FileStatus.get(fileName);
      List<Path> paths = Utilities.convertStatusToPath(files);
      ConcurrentHashMap<HboxContainerId, ConcurrentHashMap<String, InputInfo>> containersFiles = new ConcurrentHashMap<>();
      for (int i = 0, len = paths.size(); i < len; i++) {
        Integer index = i % splitWorkerNum;
        ConcurrentHashMap<String, InputInfo> mapSplit;
        HboxContainerId containerId = new HboxContainerId(acquiredWorkerContainers.get(index).getId());
        if (containersFiles.containsKey(containerId)) {
          mapSplit = containersFiles.get(containerId);
        } else {
          mapSplit = new ConcurrentHashMap<>();
          containersFiles.put(containerId, mapSplit);
        }
        if (mapSplit.containsKey(fileName)) {
          mapSplit.get(fileName).addPath(paths.get(i));
        } else {
          InputInfo inputInfo = new InputInfo();
          inputInfo.setAliasName(fileName);
          List<Path> ps = new ArrayList<>();
          ps.add(paths.get(i));
          inputInfo.setPaths(ps);
          mapSplit.put(fileName, inputInfo);
        }
      }
      Set<HboxContainerId> containerIdSet = containersFiles.keySet();
      for (HboxContainerId containerId : containerIdSet) {
        containerId2InputInfo.get(containerId).add(containersFiles.get(containerId).get(fileName));
        LOG.info("put " + fileName + " to " + containerId.toString());
      }
    }
    LOG.info("inputinfos " + new Gson().toJson(containerId2InputInfo));
  }

  private void allocateWholeInput() {
    Set<String> fileKeys = input2FileStatus.keySet();
    for (String fileName : fileKeys) {
      List<FileStatus> files = input2FileStatus.get(fileName);
      List<Path> paths = Utilities.convertStatusToPath(files);
      for (int i = 0, len = paths.size(); i < len; i++) {
        if (wholeFiles.containsKey(fileName)) {
          wholeFiles.get(fileName).addPath(paths.get(i));
        } else {
          InputInfo inputInfo = new InputInfo();
          inputInfo.setAliasName(fileName);
          List<Path> ps = new ArrayList<>();
          ps.add(paths.get(i));
          inputInfo.setPaths(ps);
          wholeFiles.put(fileName, inputInfo);
        }
      }
    }
    LOG.info("inputinfos " + new Gson().toJson(wholeFiles));
  }

  private void allocateInputSplitsInlcudePs() {
    List<Container> acquiredContainers = new ArrayList<>();
    acquiredContainers.addAll(acquiredWorkerContainers);
    acquiredContainers.addAll(acquiredPsContainers);
    for (Container container : acquiredContainers) {
      LOG.info("Initializing " + container.getId().toString() + " input splits");
      containerId2InputInfo.putIfAbsent(new HboxContainerId(container.getId()), new ArrayList<InputInfo>());
    }
    Set<String> fileKeys = input2FileStatus.keySet();
    for (String fileName : fileKeys) {
      List<FileStatus> files = input2FileStatus.get(fileName);
      List<Path> paths = Utilities.convertStatusToPath(files);
      ConcurrentHashMap<HboxContainerId, ConcurrentHashMap<String, InputInfo>> containersFiles = new ConcurrentHashMap<>();
      for (int i = 0, len = paths.size(); i < len; i++) {
        Integer index = i % (workerNum + psNum);
        ConcurrentHashMap<String, InputInfo> mapSplit;
        HboxContainerId containerId = new HboxContainerId(acquiredContainers.get(index).getId());
        if (containersFiles.containsKey(containerId)) {
          mapSplit = containersFiles.get(containerId);
        } else {
          mapSplit = new ConcurrentHashMap<>();
          containersFiles.put(containerId, mapSplit);
        }
        if (mapSplit.containsKey(fileName)) {
          mapSplit.get(fileName).addPath(paths.get(i));
        } else {
          InputInfo inputInfo = new InputInfo();
          inputInfo.setAliasName(fileName);
          List<Path> ps = new ArrayList<>();
          ps.add(paths.get(i));
          inputInfo.setPaths(ps);
          mapSplit.put(fileName, inputInfo);
        }
      }
      Set<HboxContainerId> containerIdSet = containersFiles.keySet();
      for (HboxContainerId containerId : containerIdSet) {
        containerId2InputInfo.get(containerId).add(containersFiles.get(containerId).get(fileName));
        LOG.info("put " + fileName + " to " + containerId.toString());
      }
    }
    LOG.info("inputinfos " + new Gson().toJson(containerId2InputInfo));
  }

  private void allocateInputStreamSplits() {

    for (Container container : acquiredWorkerContainers) {
      LOG.info("Initializing " + container.getId().toString() + " input splits");
      containerId2InputSplit.putIfAbsent(new HboxContainerId(container.getId()), new ArrayList<InputSplit>());
    }
    int splitWorkerNum = workerNum;
    if (tfEvaluator) {
      --splitWorkerNum;
      LOG.info("Note that current tensorflow job has the evaluator type. Not allocate the input to the last container.");
    }

    if(conf.getBoolean(HboxConfiguration.HBOX_INPUT_STREAM_SHUFFLE, HboxConfiguration.DEFAULT_HBOX_INPUT_STREAM_SHUFFLE)) {
      LOG.info("HBOX_INPUT_STREAM_SHUFFLE is true");
      for (int i = 0, len = inputFileSplits.length; i < len; i++) {
        Integer index = i % splitWorkerNum;
        HboxContainerId containerId = new HboxContainerId(acquiredWorkerContainers.get(index).getId());
        containerId2InputSplit.get(containerId).add(inputFileSplits[i]);
        LOG.info("put split " + (i+1) + " to " + containerId.toString());
      }
    } else {
      LOG.info("HBOX_INPUT_STREAM_SHUFFLE is false");
      int nsplit = inputFileSplits.length / splitWorkerNum;
      int msplit = inputFileSplits.length % splitWorkerNum;
      int count = 0;
      for (int i = 0; i < splitWorkerNum; i++) {
        HboxContainerId containerId = new HboxContainerId(acquiredWorkerContainers.get(i).getId());
        for(int j = 0; j < nsplit; j++) {
          containerId2InputSplit.get(containerId).add(inputFileSplits[count++]);
          LOG.info("put split " + count + " to " + containerId.toString());
        }
        if(msplit > 0) {
          containerId2InputSplit.get(containerId).add(inputFileSplits[count++]);
          LOG.info("put split " + count + " to " + containerId.toString());
          msplit--;
        }
      }
    }
  }

  private void buildOutputLocations() {
    String hboxOutputs = envs.get(HboxConstants.Environment.HBOX_OUTPUTS.toString());
    if (StringUtils.isBlank(hboxOutputs)) {
      return;
    }
    String[] outputs = StringUtils.split(hboxOutputs, "|");
    if (outputs != null && outputs.length > 0) {
      for (String output : outputs) {
        String outputPathTuple[] = StringUtils.split(output, "#");
        if (outputPathTuple.length < 2) {
          throw new RuntimeException("Error input path format " + hboxOutputs);
        }
        String pathRemote = outputPathTuple[0];
        OutputInfo outputInfo = new OutputInfo();
        outputInfo.setDfsLocation(pathRemote);
        String pathLocal;
        if(hboxAppType.equals("MPI") || hboxAppType.equals("HOROVOD")) {
          pathLocal = mpiExecDir + File.separator + outputPathTuple[1];
        } else {
          pathLocal = outputPathTuple[1];
        }
        outputInfo.setLocalLocation(pathLocal);
        outputInfos.add(outputInfo);
        LOG.info("Application output " + pathRemote + "#" + pathLocal);
      }
    } else {
      throw new RuntimeException("Error input path format " + hboxOutputs);
    }
  }

  private void registerApplicationMaster() {
    try {
      amrmAsync.registerApplicationMaster(this.messageService.getServerAddress().getHostName(),
          this.messageService.getServerAddress().getPort(), applicationMasterTrackingUrl);
    } catch (Exception e) {
      throw new RuntimeException("Registering application master failed,", e);
    }
  }

  private void buildContainerRequest(String[] hostLocals) {
    if(conf.getBoolean(HboxConfiguration.HBOX_HOST_LOCAL_ENABLE, HboxConfiguration.DEFAULT_HBOX_HOST_LOCAL_ENABLE)) {
      HboxConfiguration hboxConf = new HboxConfiguration();
      String hostLocaldir = hboxConf.get("fs.defaultFS") + conf.get(HboxConfiguration.HBOX_HISTORY_LOG_DIR,
              HboxConfiguration.DEFAULT_HBOX_HISTORY_LOG_DIR) + "/" + conf.get("hadoop.job.ugi").split(",")[0]
              + "/" + envs.get(HboxConstants.Environment.HBOX_APP_NAME.toString());
      Path hostLocalPath = new Path(hostLocaldir);
      String line;
      try {
        if(hostLocalPath.getFileSystem(hboxConf).exists(hostLocalPath)) {
          FSDataInputStream in = hostLocalPath.getFileSystem(hboxConf).open(hostLocalPath);
          BufferedReader br = new BufferedReader(new InputStreamReader(in));
          line = br.readLine();
          hostLocals = line.split(",");
          LOG.info("now in buildContainerRequest, host local is: " + Arrays.toString(hostLocals));
          in.close();
        }
      } catch (IOException e) {
        LOG.info("open and read the host local from " + hostLocalPath + " error, " + e);
      }
    }
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(appPriority);
    Resource workerCapability = Records.newRecord(Resource.class);
    int workerOverheadMem = (int) Math.max(workerMemory * conf.getDouble(HboxConfiguration.HBOX_MEMORY_OVERHEAD_FRACTION, HboxConfiguration.DEFAULT_HBOX_MEMORY_OVERHEAD_FRACTION),
        conf.getInt(HboxConfiguration.HBOX_MEMORY_OVERHEAD_MINIMUM, HboxConfiguration.DEFAULT_HBOX_MEMORY_OVERHEAD_MINIMUM));
    workerCapability.setMemory(Math.min(workerMemory + workerOverheadMem, maxContainerMem));
    workerCapability.setVirtualCores(workerVCores);
    workerCapability.setGpuCores(workerGCores);
    workerContainerRequest = new ContainerRequest(workerCapability, hostLocals, null, priority, true, conf.get(HboxConfiguration.HBOX_JOB_LABEL_NAME));
    LOG.info("Create worker container request: " + workerContainerRequest.toString());

    if("TENSORFLOW".equals(hboxAppType) && !single) {
      Resource psCapability = Records.newRecord(Resource.class);
      int psOverheadMem = (int) Math.max(workerMemory * conf.getDouble(HboxConfiguration.HBOX_MEMORY_OVERHEAD_FRACTION, HboxConfiguration.DEFAULT_HBOX_MEMORY_OVERHEAD_FRACTION),
          conf.getInt(HboxConfiguration.HBOX_MEMORY_OVERHEAD_MINIMUM, HboxConfiguration.DEFAULT_HBOX_MEMORY_OVERHEAD_MINIMUM));
      psCapability.setMemory(Math.min(psMemory + psOverheadMem, maxContainerMem));
      psCapability.setVirtualCores(psVCores);
      psCapability.setGpuCores(psGCores);
      psContainerRequest = new ContainerRequest(psCapability, hostLocals, null, priority, true, conf.get(HboxConfiguration.HBOX_JOB_LABEL_NAME));
      LOG.info("Create ps container request: " + psContainerRequest.toString());
    }

    if("MXNET".equals(hboxAppType) && !singleMx) {
      Resource psCapability = Records.newRecord(Resource.class);
      psCapability.setMemory(psMemory);
      psCapability.setVirtualCores(psVCores);
      psCapability.setGpuCores(psGCores);
      psContainerRequest = new ContainerRequest(psCapability, hostLocals, null, priority);
      LOG.info("Create ps container request: " + psContainerRequest.toString());
    }

    if ("DISTLIGHTLDA".equals(hboxAppType)) {
      Resource psCapability = Records.newRecord(Resource.class);
      psCapability.setMemory(psMemory);
      psCapability.setVirtualCores(psVCores);
      psCapability.setGpuCores(psGCores);
      psContainerRequest = new ContainerRequest(psCapability, hostLocals, null, priority);
      LOG.info("Create ps container request: " + psContainerRequest.toString());
    }

    if ("XFLOW".equals(hboxAppType)) {
      Resource psCapability = Records.newRecord(Resource.class);
      psCapability.setMemory(psMemory);
      psCapability.setVirtualCores(psVCores);
      psCapability.setGpuCores(psGCores);
      psContainerRequest = new ContainerRequest(psCapability, hostLocals, null, priority);
      LOG.info("Create ps container request: " + psContainerRequest.toString());
    }
  }

  private void buildContainerLocalResource() {
    URI defaultUri = new Path(conf.get("fs.defaultFS")).toUri();
    LOG.info("default URI is " + defaultUri.toString());
    containerLocalResource = new HashMap<>();
    try {
      containerLocalResource.put(HboxConstants.HBOX_APPLICATION_JAR,
          Utilities.createApplicationResource(appJarRemoteLocation.getFileSystem(conf),
              appJarRemoteLocation,
              LocalResourceType.FILE));
      containerLocalResource.put(HboxConstants.HBOX_JOB_CONFIGURATION,
          Utilities.createApplicationResource(appConfRemoteLocation.getFileSystem(conf),
              appConfRemoteLocation,
              LocalResourceType.FILE));

      if (appCacheFilesRemoteLocation != null) {
        String[] cacheFiles = StringUtils.split(appCacheFilesRemoteLocation, ",");
        for (String path : cacheFiles) {
          Path pathRemote;
          String aliasName;
          if (path.contains("#")) {
            String[] paths = StringUtils.split(path, "#");
            if (paths.length != 2) {
              throw new RuntimeException("Error cacheFile path format " + appCacheFilesRemoteLocation);
            }
            pathRemote = new Path(paths[0]);
            aliasName = paths[1];
          } else {
            pathRemote = new Path(path);
            aliasName = pathRemote.getName();
          }
          URI pathRemoteUri = pathRemote.toUri();
          if (Boolean.parseBoolean(conf.get(HboxConfiguration.HBOX_APPEND_DEFAULTFS_ENABLE, String.valueOf(HboxConfiguration.DEFAULT_HBOX_APPEND_DEFAULTFS_ENABLE)))) {
            if (pathRemoteUri.getScheme() == null || pathRemoteUri.getHost() == null) {
              pathRemote = new Path(defaultUri.toString(), pathRemote.toString());
            }
          }
          LOG.info("Cache file remote path is " + pathRemote + " and alias name is " + aliasName);
          containerLocalResource.put(aliasName,
              Utilities.createApplicationResource(pathRemote.getFileSystem(conf),
                  pathRemote,
                  LocalResourceType.FILE));
          if(hboxAppType.equals("MPI") || hboxAppType.equals("HOROVOD")) {
            reLinkFiles.append(aliasName).append(",");
          }
        }
      }

      if (appCacheArchivesRemoteLocation != null) {
        String[] cacheArchives = StringUtils.split(appCacheArchivesRemoteLocation, ",");
        for (String path : cacheArchives) {
          Path pathRemote;
          String aliasName;
          if (path.contains("#")) {
            String[] paths = StringUtils.split(path, "#");
            if (paths.length != 2) {
              throw new RuntimeException("Error cacheArchive path format " + appCacheArchivesRemoteLocation);
            }
            pathRemote = new Path(paths[0]);
            aliasName = paths[1];
          } else {
            pathRemote = new Path(path);
            aliasName = pathRemote.getName();
          }
          URI pathRemoteUri = pathRemote.toUri();
          if (Boolean.parseBoolean(conf.get(HboxConfiguration.HBOX_APPEND_DEFAULTFS_ENABLE, String.valueOf(HboxConfiguration.DEFAULT_HBOX_APPEND_DEFAULTFS_ENABLE)))) {
            if (pathRemoteUri.getScheme() == null || pathRemoteUri.getHost() == null) {
              pathRemote = new Path(defaultUri.toString(), pathRemote.toString());
            }
          }
          LOG.info("Cache archive remote path is " + pathRemote + " and alias name is " + aliasName);
          containerLocalResource.put(aliasName,
              Utilities.createApplicationResource(pathRemote.getFileSystem(conf),
                  pathRemote,
                  LocalResourceType.ARCHIVE));
          if(hboxAppType.equals("MPI") || hboxAppType.equals("HOROVOD")) {
            reLinkFiles.append(aliasName).append(",");
          }
        }
      }

      if (appFilesRemoteLocation != null) {
        String[] tfFiles = StringUtils.split(appFilesRemoteLocation, ",");
        for (String file : tfFiles) {
          Path path = new Path(file);
          containerLocalResource.put(path.getName(),
              Utilities.createApplicationResource(path.getFileSystem(conf),
                  path,
                  LocalResourceType.FILE));
          if(hboxAppType.equals("MPI") || hboxAppType.equals("HOROVOD")) {
            reLinkFiles.append(path.getName()).append(",");
          }
        }
      }

      if(appLibJarsRemoteLocation != null) {
        String[] tfFiles = StringUtils.split(appLibJarsRemoteLocation, ",");
        for (String file : tfFiles) {
          Path path = new Path(file);
          containerLocalResource.put(path.getName(),
                  Utilities.createApplicationResource(path.getFileSystem(conf),
                          path,
                          LocalResourceType.FILE));
          libJarsClassPath += path.getName() + ":";
        }
      }

    } catch (IOException e) {
      throw new RuntimeException("Error while build container local resource", e);
    }
  }

  private Map<String, String> buildContainerEnv(String role) {
    LOG.info("Seting environments for the Container");
    Map<String, String> containerEnv = new HashMap<>();
    String containerType = conf.get(HboxConfiguration.CONTAINER_EXECUTOR_TYPE,
            HboxConfiguration.DEFAULT_CONTAINER_EXECUTOR_TYPE);
    containerEnv.put(HboxConstants.Environment.HADOOP_USER_NAME.toString(), conf.get("hadoop.job.ugi").split(",")[0]);
    containerEnv.put(HboxConstants.Environment.HBOX_TF_ROLE.toString(), role);
    containerEnv.put(HboxConstants.Environment.HBOX_CONTAINER_EXECUTOR_TYPE.toString(), containerType);
    if (this.inputPath.length() > 0) {
      containerEnv.put(HboxConstants.Environment.HBOX_INPUT_PATH.toString(), this.inputPath.substring(0, inputPath.length() - 1));
    }

    if(hboxAppType.equals("VPC") || hboxAppType.equals("DIGITS") || containerType.toUpperCase().equals("DOCKER")) {
      String imageName = conf.get(HboxConfiguration.DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME,
              HboxConfiguration.DEFALUT_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);
      if(hboxAppType.equals("DIGITS")) {
        imageName = conf.get(HboxConfiguration.HBOX_DIGITS_IMAGE_NAME,
                HboxConfiguration.DEFAULT_HBOX_DIGITS_IMAGE_NAME);
      }
      containerEnv.put(HboxConstants.Environment.HBOX_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME.toString(), imageName);
      containerEnv.put(HboxConstants.Environment.HBOX_DOCKER_CONTAINER_EXECUTOR_EXEC_NAME.toString(), conf.get(HboxConfiguration.DOCKER_CONTAINER_EXECUTOR_EXEC_NAME,
              HboxConfiguration.DEFAULT_DOCKER_CONTAINER_EXECUTOR_EXEC_NAME));
      containerEnv.put(HboxConstants.Environment.HBOX_CONTAINER_EXECUTOR_TYPE.toString(), "docker");
    }

    if(!hboxAppType.equals("VPC") && !hboxAppType.equals("DIGITS")) {
      containerEnv.put(HboxConstants.Environment.HBOX_EXEC_CMD.toString(), hboxCommand);
    }
    containerEnv.put(HboxConstants.Environment.HBOX_APP_TYPE.toString(), hboxAppType);
    if(hboxAppType.equals("MXNET") && !singleMx) {
      containerEnv.put(HboxConstants.Environment.HBOX_DMLC_WORKER_NUM.toString(), String.valueOf(workerNum));
      containerEnv.put(HboxConstants.Environment.HBOX_DMLC_SERVER_NUM.toString(), String.valueOf(psNum));
      containerEnv.put("DMLC_PS_ROOT_URI", dmlcPsRootUri);
      containerEnv.put("DMLC_PS_ROOT_PORT", String.valueOf(dmlcPsRootPort));
    }

    if(hboxAppType.equals("DISTXGBOOST")) {
      containerEnv.put("DMLC_NUM_WORKER", String.valueOf(workerNum));
      containerEnv.put("DMLC_TRACKER_URI", dmlcTrackerUri);
      containerEnv.put("DMLC_TRACKER_PORT", String.valueOf(dmlcTrackerPort));
    }

    if(hboxAppType.equals("DISTLIGHTGBM")) {
      containerEnv.put(HboxConstants.Environment.HBOX_LIGHTGBM_WORKER_NUM.toString(), String.valueOf(workerNum));
    }

    if(hboxAppType.equals("DISTLIGHTLDA")) {
      containerEnv.put(HboxConstants.Environment.HBOX_LIGHTLDA_WORKER_NUM.toString(), String.valueOf(workerNum));
      containerEnv.put(HboxConstants.Environment.HBOX_LIGHTLDA_PS_NUM.toString(), String.valueOf(psNum));
    }

    if(hboxAppType.equals("XFLOW")){
      containerEnv.put(HboxConstants.Environment.HBOX_DMLC_WORKER_NUM.toString(), String.valueOf(workerNum));
      containerEnv.put(HboxConstants.Environment.HBOX_DMLC_SERVER_NUM.toString(), String.valueOf(psNum));
      containerEnv.put("DMLC_PS_ROOT_URI", dmlcPsRootUri);
      containerEnv.put("DMLC_PS_ROOT_PORT", String.valueOf(dmlcPsRootPort));
    }

    if (hboxAppType.equals("DISTTORCH")) {
      containerEnv.put("WORLD_SIZE", String.valueOf(workerNum));
    }

    if(conf.getBoolean(HboxConfiguration.HBOX_USER_CLASSPATH_FIRST, HboxConfiguration.DEFAULT_HBOX_USER_CLASSPATH_FIRST)) {
      containerEnv.put("CLASSPATH", libJarsClassPath + System.getenv("CLASSPATH"));
    } else {
      containerEnv.put("CLASSPATH",  System.getenv("CLASSPATH") + ":" + libJarsClassPath);
    }
    containerEnv.put(HboxConstants.Environment.APP_ATTEMPTID.toString(), applicationAttemptID.toString());
    containerEnv.put(HboxConstants.Environment.APP_ID.toString(), applicationAttemptID.getApplicationId().toString());

    containerEnv.put(HboxConstants.Environment.APPMASTER_HOST.toString(),
        System.getenv(ApplicationConstants.Environment.NM_HOST.toString()));
    containerEnv.put(HboxConstants.Environment.APPMASTER_PORT.toString(),
        String.valueOf(containerListener.getServerPort()));
    containerEnv.put("PATH", System.getenv("PATH") + ":" + System.getenv(HboxConstants.Environment.USER_PATH.toString()));

    if(hboxAppType.equals("MPI") || hboxAppType.equals("HOROVOD")) {
      if (!mpiExecDir.equals("")) {
        containerEnv.put(HboxConstants.Environment.MPI_EXEC_DIR.toString(), mpiExecDir);
      }
      if (reLinkFiles.length() > 0) {
        containerEnv.put(HboxConstants.Environment.MPI_FILES_LINKS.toString(), reLinkFiles.substring(0, reLinkFiles.length() - 1));
      }
    }

    LOG.debug("env:" + containerEnv.toString());
    Set<String> envStr = containerEnv.keySet();
    for (String anEnvStr : envStr) {
      LOG.debug("env:" + anEnvStr);
    }

    if (conf.get(HboxConfiguration.HBOX_CONTAINER_ENV) != null) {
      String[] containerUserEnv = StringUtils.split(conf.get(HboxConfiguration.HBOX_CONTAINER_ENV), "|");
      if (containerUserEnv.length > 0) {
        for (String envPair : containerUserEnv) {
          String[] env = StringUtils.split(envPair, "=");
          if (env.length != 2) {
            LOG.error(envPair + " is not the correct.");
          } else {
            Utilities.addPathToEnvironment(containerEnv, env[0], env[1]);
          }
        }
      }
    }
    return containerEnv;
  }

  private List<String> buildContainerLaunchCommand(int containerMemory) {
    List<String> containerLaunchcommands = new ArrayList<>();
    LOG.info("Setting up container command");
    Vector<CharSequence> vargs = new Vector<>(10);
    int jvmContainerMem = (int) Math.max(containerMemory * conf.getDouble(HboxConfiguration.HBOX_CONTAINER_JVM_MEMORY_FRACTION, HboxConfiguration.DEFAULT_HBOX_CONTAINER_JVM_MEMORY_FRACTION),
        conf.getInt(HboxConfiguration.HBOX_CONTAINER_JVM_MEMORY_MINIMUM, HboxConfiguration.DEFAULT_HBOX_CONTAINER_JVM_MEMORY_MINIMUM));
    if (jvmContainerMem > containerMemory)
      jvmContainerMem = containerMemory;
    vargs.add("${JAVA_HOME}" + "/bin/java");
    vargs.add("-Xmx" + jvmContainerMem + "m");
    vargs.add("-Xms" + jvmContainerMem + "m");
    String javaOpts = conf.get(HboxConfiguration.HBOX_CONTAINER_EXTRA_JAVA_OPTS, HboxConfiguration.DEFAULT_HBOX_CONTAINER_JAVA_OPTS_EXCEPT_MEMORY);
    if (!StringUtils.isBlank(javaOpts)) {
      vargs.add(javaOpts);
    }
    vargs.add("net.qihoo.hbox.container.HboxContainer");
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDOUT);
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDERR);

    StringBuilder containerCmd = new StringBuilder();
    for (CharSequence str : vargs) {
      containerCmd.append(str).append(" ");
    }
    containerLaunchcommands.add(containerCmd.toString());
    LOG.info("Container launch command: " + containerLaunchcommands.toString());
    return containerLaunchcommands;
  }

  /**
   * Application Master launch "mpiexec" process locally for mpi app
   */
  private void launchMpiExec() throws IOException {
    LOG.info("Launching mpiexec in Application Master");
    StringBuilder commandBuilder = new StringBuilder();
    StringBuilder ldLibraryPath = new StringBuilder();

    String mpiExtraLdLibraryPath = conf.get(HboxConfiguration.HBOX_MPI_EXTRA_LD_LIBRARY_PATH);
    if (mpiExtraLdLibraryPath != null) {
      ldLibraryPath.append(mpiExtraLdLibraryPath);
      LOG.info("add " + ldLibraryPath + " to LD_LIBRARY_PATH");
    }
    if (conf.getBoolean(HboxConfiguration.HBOX_MPI_INSTALL_DIR_ENABLE, HboxConfiguration.DEFAULT_HBOX_MPI_INSTALL_DIR_ENABLE)) {
      String mpiInstallDir = conf.get(HboxConfiguration.HBOX_MPI_INSTALL_DIR, HboxConfiguration.DEFAULT_HBOX_MPI_INSTALL_DIR);
      commandBuilder.append(mpiInstallDir).append(File.separator).append("bin").append(File.separator);
      ldLibraryPath.append(":").append(mpiInstallDir).append(File.separator).append("lib");
    }
    commandBuilder.append("mpiexec --host ");
    ldLibraryPath.append(":").append(System.getenv("LD_LIBRARY_PATH"));
    for (Container container : acquiredWorkerContainers) {
      commandBuilder.append(container.getNodeId().getHost()).append(",");
    }
    commandBuilder.deleteCharAt(commandBuilder.length() - 1);
    commandBuilder.append(" ").append(hboxCommand);

    String[] envs = new String[]{
            "PATH=" + System.getenv("PATH"),
            "PWD=" + mpiExecDir,
            "LD_LIBRARY_PATH=" + ldLibraryPath.toString()
    };

    File mpiExec = new File(mpiExecDir);
    LOG.info("Executing mpi exec command: " + commandBuilder.toString());
    Runtime rt = Runtime.getRuntime();

    StringTokenizer tokenizer = new StringTokenizer(commandBuilder.toString());
    String[] commandArray = new String[tokenizer.countTokens()];
    for (int i = 0; tokenizer.hasMoreTokens(); i++) {
      commandArray[i] = tokenizer.nextToken();
    }
    LOG.info("Mpi exec Process run in: " + mpiExec.toString());
    mpiExecProcess = rt.exec(commandArray, envs, mpiExec);

    Thread stdinThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          BufferedReader reader;
          reader = new BufferedReader(new InputStreamReader(mpiExecProcess.getInputStream()));
          String mpiExecOutput;
          while ((mpiExecOutput = reader.readLine()) != null) {
            if (mpiExecOutput.startsWith("command")) {
              LOG.info("Container mpi Command " + mpiExecOutput);
              appendMessage(new Message(LogType.STDERR, mpiExecOutput));
              mpiContainerCommand = mpiExecOutput.replaceFirst("command:", "");
              if (conf.getBoolean(HboxConfiguration.HBOX_CONTAINER_RUNNING_LOG_ENABLE, HboxConfiguration.DEFAULT_HBOX_CONTAINER_RUNNING_LOG_ENABLE)) {
                amContainerStdOut.append(mpiExecOutput);
              }
            } else {
              LOG.info(mpiExecOutput);
              appendMessage(new Message(LogType.STDOUT, mpiExecOutput));
            }
          }
        } catch (Exception e) {
          LOG.warn("Error in mpi exec process stdinThread");
        }
      }
    });
    stdinThread.start();

    Thread stderrThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          BufferedReader reader;
          reader = new BufferedReader(new InputStreamReader(mpiExecProcess.getErrorStream()));
          String mpiExecStderr;
          while ((mpiExecStderr = reader.readLine()) != null) {
            LOG.info(mpiExecStderr);
            appendMessage(new Message(LogType.STDERR, mpiExecStderr));
            if (conf.getBoolean(HboxConfiguration.HBOX_CONTAINER_RUNNING_LOG_ENABLE, HboxConfiguration.DEFAULT_HBOX_CONTAINER_RUNNING_LOG_ENABLE)) {
              amContainerStdErr.append(mpiExecStderr);
            }
          }
        } catch (Exception e) {
          LOG.warn("Error in mpi exec process stderrThread");
        }
      }
    });
    stderrThread.start();
  }
  //read user horovod config parameter
  private String readHorovodConfig(){
    StringBuilder horovodConfig = new StringBuilder();
    if(conf.getBoolean(HboxConfiguration.HBOX_HOROVOD_MPI_THREADS_DISABLE, HboxConfiguration.DEFAULT_HBOX_HOROVOD_MPI_THREADS_DISABLE)){
      horovodConfig.append("-x HOROVOD_MPI_THREADS_DISABLE=1 ");
    }
    String timelineFileName = conf.get(HboxConfiguration.HBOX_HOROVOD_TIMELINE);
    if(timelineFileName != null){
      horovodConfig.append("-x HOROVOD_TIMELINE=").append(timelineFileName).append(" ");
    }
    int fusionThreshold = conf.getInt(HboxConfiguration.HBOX_HOROVOD_FUSION_THRESHOLD, HboxConfiguration.DEFAULT_HBOX_HOROVOD_FUSION_THRESHOLD);
    if(fusionThreshold != -1){
      horovodConfig.append("-x HOROVOD_FUSION_THRESHOLD=").append(fusionThreshold).append(" ");
    }
    int cycleTime = conf.getInt(HboxConfiguration.HBOX_HOROVOD_CYCLE_TIME, HboxConfiguration.DEFAULT_HBOX_HOROVOD_CYCLE_TIME);
    if(cycleTime != -1){
      horovodConfig.append("-x HOROVOD_CYCLE_TIME=").append(cycleTime).append(" ");
    }
    if(conf.getBoolean(HboxConfiguration.HBOX_HOROVOD_STALL_CHECK_DISABLE, HboxConfiguration.DEFAULT_HBOX_HOROVOD_STALL_CHECK_DISABLE)) {
      horovodConfig.append("-x HOROVOD_STALL_CHECK_DISABLE=1 ");
    }
    if(conf.getBoolean(HboxConfiguration.HBOX_HOROVOD_HIERARCHICAL_ALLREDUCE, HboxConfiguration.DEFAULT_HBOX_HOROVOD_HIERARCHICAL_ALLREDUCE)){
      horovodConfig.append("-x HOROVOD_HIERARCHICAL_ALLREDUCE=1 ");
    }
    return horovodConfig.toString().trim();
  }

  //launch horovod mpi task
  private void launchHorovodExec() throws IOException {
    LOG.info("Launching horovod exec in Application Master");
    StringBuilder commandBuilder = new StringBuilder();
    StringBuilder ldLibraryPath = new StringBuilder();

    String mpiExtraLdLibraryPath = conf.get(HboxConfiguration.HBOX_HOROVOD_EXTRA_LD_LIBRARY_PATH);
    if (mpiExtraLdLibraryPath != null) {
      ldLibraryPath.append(mpiExtraLdLibraryPath);
      LOG.info("add " + ldLibraryPath + " to LD_LIBRARY_PATH");
    }
    if (conf.getBoolean(HboxConfiguration.HBOX_MPI_INSTALL_DIR_ENABLE, HboxConfiguration.DEFAULT_HBOX_MPI_INSTALL_DIR_ENABLE)) {
      String mpiInstallDir = conf.get(HboxConfiguration.HBOX_MPI_INSTALL_DIR, HboxConfiguration.DEFAULT_HBOX_MPI_INSTALL_DIR);
      commandBuilder.append(mpiInstallDir).append(File.separator).append("bin").append(File.separator);
      ldLibraryPath.append(":").append(mpiInstallDir).append(File.separator).append("lib");
    }
    int processPerWorker = conf.getInt(HboxConfiguration.HBOX_HOROVOD_PROCESS_NUM_PER_WORKER,HboxConfiguration.DEDAULT_HBOX_HOROVOD_PROCESS_NUM_PER_WORKER);
    commandBuilder.append("mpirun -np ").append(workerNum * processPerWorker).append(" -H ");
    ldLibraryPath.append(":").append(System.getenv("LD_LIBRARY_PATH"));
    for (Container container : acquiredWorkerContainers) {
      if(processPerWorker == 1)
        commandBuilder.append(container.getNodeId().getHost()).append(",");
      else
        commandBuilder.append(container.getNodeId().getHost()).append(":").append(processPerWorker).append(",");
    }
    commandBuilder.deleteCharAt(commandBuilder.length() - 1);
    String horovodConfig = readHorovodConfig();
    if(horovodConfig.trim().equals(""))
      commandBuilder.append(" ");
    else
      commandBuilder.append(" ").append(readHorovodConfig()).append(" ");
    commandBuilder.append("-bind-to none -map-by slot -x NCCL_DEBUG=INFO -x LD_LIBRARY_PATH -x PATH -mca pml ob1 -mca btl ^openib ");
    commandBuilder.append(hboxCommand);

    List<String> envs = new ArrayList<>(20);
    Map<String, String> userEnv = new HashMap<>();
    if (conf.get(HboxConfiguration.HBOX_CONTAINER_ENV) != null) {
      String[] env = StringUtils.split(conf.get(HboxConfiguration.HBOX_CONTAINER_ENV), "|");
      for (String envPair : env) {
        String[] userEnvPair = StringUtils.split(envPair, "=");
        if (userEnvPair.length != 2) {
          LOG.error(envPair + " is not correct");
        } else {
          envs.add(envPair);
          userEnv.put(userEnvPair[0], userEnvPair[1]);
        }
      }
    }
    envs.add("PWD=" + mpiExecDir);
    if (userEnv.containsKey("PATH")) {
      envs.add("PATH=" + userEnv.get("PATH") + System.getProperty("path.separator") + System.getenv("PATH"));
    } else {
      envs.add("PATH=" + System.getenv("PATH"));
    }
    if (userEnv.containsKey("LD_LIBRARY_PATH")) {
      envs.add("LD_LIBRARY_PATH=" + userEnv.get("LD_LIBRARY_PATH") + System.getProperty("path.separator") + ldLibraryPath.toString());
    } else {
      envs.add("LD_LIBRARY_PATH=" + ldLibraryPath.toString());
    }

    File mpiExec = new File(mpiExecDir);
    LOG.info("Executing horovod exec command: " + commandBuilder.toString());
    Runtime rt = Runtime.getRuntime();

    StringTokenizer tokenizer = new StringTokenizer(commandBuilder.toString());
    String[] commandArray = new String[tokenizer.countTokens()];
    for (int i = 0; tokenizer.hasMoreElements(); i++) {
      commandArray[i] = tokenizer.nextToken();
    }
    LOG.info("Horovod mpi exec Process run in: " + mpiExec.toString());
    mpiExecProcess = rt.exec(commandArray, envs.toArray(new String[envs.size()]), mpiExec);

    Thread stdinThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          BufferedReader reader;
          reader = new BufferedReader(new InputStreamReader(mpiExecProcess.getInputStream()));
          String mpiExecOutput;
          while ((mpiExecOutput = reader.readLine()) != null) {
            if (mpiExecOutput.startsWith("command")) {
              LOG.info("Container horovod Command " + mpiExecOutput);
              appendMessage(new Message(LogType.STDERR, mpiExecOutput));
              //get orted command
              mpiContainerCommand = mpiExecOutput.replaceFirst("command:", "");
            } else {
              LOG.info(mpiExecOutput);
              appendMessage(new Message(LogType.STDOUT, mpiExecOutput));
            }
          }
        } catch(Exception e) {
          LOG.warn("Error in horovod exec process stdinThread");
        }
      }
    });
    stdinThread.start();

    Thread stderrThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          BufferedReader reader;
          reader = new BufferedReader(new InputStreamReader(mpiExecProcess.getErrorStream()));
          String mpiExecStderr;
          while ((mpiExecStderr = reader.readLine()) != null) {
            LOG.info(mpiExecStderr);
            appendMessage(new Message(LogType.STDERR, mpiExecStderr));
          }
        } catch (Exception e) {
          LOG.warn("Error in mpi exec process stderrThread");
        }
      }
    });
    stderrThread.start();
  }

  /**
   * Async Method telling NMClientAsync to launch specific container
   *
   * @param container  the container which should be launched
   * @return is launched success
   */
  @SuppressWarnings("deprecation")
  private void launchContainer(Map<String, LocalResource> containerLocalResource,
                               Map<String, String> containerEnv,
                               List<String> containerLaunchcommands,
                               Container container, int index) throws IOException {
    LOG.info("Setting up launch context for containerid="
        + container.getId());
    if(hboxAppType.equals("MPI") || hboxAppType.equals("HOROVOD")) {
      String containerMpiCommand = mpiContainerCommand.replace("<template>",
              String.valueOf(index)).replaceAll("\"", "#");

      containerEnv.put(HboxConstants.Environment.CONTAINER_COMMAND.toString(), containerMpiCommand);
      LOG.info("Container mpi command is:" + containerMpiCommand);
    }
    containerEnv.put(HboxConstants.Environment.HBOX_TF_INDEX.toString(), String.valueOf(index));
    ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
        containerLocalResource, containerEnv, containerLaunchcommands, null, null, null);

    try {
      nmAsync.startContainerAsync(container, ctx);
    } catch (Exception e) {
      throw new RuntimeException("Launching container " + container.getId() + " failed!");
    }
  }

  private void appendMessage(String message, boolean logEnable) {
    if (logEnable) {
      LOG.info(message);
    }
    appendMessage(new Message(LogType.STDERR, message));
  }

  private void appendMessage(Message message) {
    if (applicationMessageQueue.size() >= conf.getInt(HboxConfiguration.HBOX_MESSAGES_LEN_MAX, HboxConfiguration.DEFAULT_HBOX_MESSAGES_LEN_MAX)) {
      applicationMessageQueue.poll();
    }
    if (!applicationMessageQueue.offer(message)) {
      LOG.warn("Message queue is full, this message will be ingored");
    }
  }

  private void unregisterApp(FinalApplicationStatus finalStatus, String diagnostics) {
    try {
      amrmAsync.unregisterApplicationMaster(finalStatus, diagnostics,
          applicationHistoryUrl);
      amrmAsync.stop();
    } catch (Exception e) {
      LOG.error("Error while unregistering Application", e);
    }
  }

  public Configuration getConf() {
    return conf;
  }

  @SuppressWarnings("deprecation")
  private boolean run() throws IOException, NoSuchAlgorithmException {
    LOG.info("ApplicationMaster Starting ...");

    registerApplicationMaster();
    if(conf.getBoolean(HboxConfiguration.HBOX_INPUT_STREAM, HboxConfiguration.DEFAULT_HBOX_INPUT_STREAM) ||
         conf.get(HboxConfiguration.HBOX_INPUT_STRATEGY, HboxConfiguration.DEFAULT_HBOX_INPUT_STRATEGY).equals("STREAM")) {
      buildInputStreamFileStatus();
    } else {
      buildInputFileStatus();
    }

    if ("TENSORFLOW".equals(hboxAppType) || "MXNET".equals(hboxAppType) || "DISTLIGHTLDA".equals(hboxAppType) || "XFLOW".equals(hboxAppType)) {
      this.appendMessage("Hbox application needs " + workerNum + " worker and "
          + psNum + " ps  containers in fact", true);
    } else {
      this.appendMessage("Hbox application needs " + workerNum + " worker container in fact", true);
    }

    buildContainerRequest(hostLocals);

    rmCallbackHandler.setNeededPsContainersCount(psNum);
    rmCallbackHandler.setNeededWorkerContainersCount(workerNum);
    rmCallbackHandler.setHboxAppType(hboxAppType);

    int allocateInterval = conf.getInt(HboxConfiguration.HBOX_ALLOCATE_INTERVAL, HboxConfiguration.DEFAULT_HBOX_ALLOCATE_INTERVAL);
    amrmAsync.setHeartbeatInterval(allocateInterval);

    for (int i = 0; i < psNum; i++) {
      amrmAsync.addContainerRequest(psContainerRequest);
    }

    if("TENSORFLOW".equals(hboxAppType) && !single) {
      LOG.info("Try to allocate " + psNum + " ps containers");
    }

    if("MXNET".equals(hboxAppType) && !singleMx) {
      LOG.info("Try to allocate " + psNum + " ps containers");
    }

    if("DISTLIGHTLDA".equals(hboxAppType)) {
      LOG.info("Try to allocate " + psNum + " ps containers");
    }

    if("XFLOW".equals(hboxAppType)) {
      LOG.info("Try to allocate " + psNum + " ps containers");
    }

    Boolean startAllocatedContainer = false;
    Long startAllocatedTimeStamp = Long.MIN_VALUE;
    while (rmCallbackHandler.getAllocatedPsContainerNumber() < psNum) {
      List<Container> cancelContainers = rmCallbackHandler.getCancelContainer();
      List<String> blackHosts = rmCallbackHandler.getBlackHosts();
      amrmAsync.updateBlacklist(blackHosts, null);
      if (cancelContainers.size() != 0) {
        for (Container container : cancelContainers) {
          LOG.info("Canceling container: " + container.getId().toString());
          amrmAsync.releaseAssignedContainer(container.getId());
          amrmAsync.addContainerRequest(psContainerRequest);
        }
        cancelContainers.clear();
      }
      if (rmCallbackHandler.getAllocatedPsContainerNumber() > 0 && !startAllocatedContainer) {
        startAllocatedContainer = true;
        startAllocatedTimeStamp = System.currentTimeMillis();
      }
      if (startAllocatedContainer && (System.currentTimeMillis() - startAllocatedTimeStamp) > conf.getInt(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS, YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS)) {
        String failMessage = "Container waiting except the allocated expiry time. Maybe the Cluster resource not satisfied the user necessary. Please resubmit !";
        LOG.info(failMessage);
        this.appendMessage("Unregister  Application", true);
        unregisterApp(FinalApplicationStatus.FAILED, failMessage);
        return false;
      }
      Utilities.sleep(allocateInterval);
    }

    if ("TENSORFLOW".equals(hboxAppType) && !single) {
      LOG.info("Total " + rmCallbackHandler.getAllocatedPsContainerNumber() + " ps containers has allocated.");
    }

    if ("MXNET".equals(hboxAppType) && !singleMx) {
      LOG.info("Total " + rmCallbackHandler.getAllocatedPsContainerNumber() + " ps containers has allocated.");
    }

    if ("DISTLIGHTLDA".equals(hboxAppType)) {
      LOG.info("Total " + rmCallbackHandler.getAllocatedPsContainerNumber() + " ps containers has allocated.");
    }

    if ("XFLOW".equals(hboxAppType)) {
      LOG.info("Total " + rmCallbackHandler.getAllocatedPsContainerNumber() + " ps containers has allocated.");
    }

    rmCallbackHandler.setWorkerContainersAllocating();

    for (int i = 0; i < workerNum; i++) {
      amrmAsync.addContainerRequest(workerContainerRequest);
    }

    LOG.info("Try to allocate " + workerNum + " worker containers");

    while (rmCallbackHandler.getAllocatedWorkerContainerNumber() < workerNum) {
      if(hboxAppType.equals("MPI") || hboxAppType.equals("HOROVOD")) {
        rmCallbackHandler.addBlackHost(applicationMasterHostname);
        List<String> blackAMs = rmCallbackHandler.getBlackHosts();
        amrmAsync.updateBlacklist(blackAMs, null);
      }
      List<Container> cancelContainers = rmCallbackHandler.getCancelContainer();
      List<String> blackHosts = rmCallbackHandler.getBlackHosts();
      amrmAsync.updateBlacklist(blackHosts, null);
      if (cancelContainers.size() != 0) {
        for (Container container : cancelContainers) {
          LOG.info("Canceling container: " + container.getId().toString());
          amrmAsync.releaseAssignedContainer(container.getId());
          amrmAsync.addContainerRequest(workerContainerRequest);
        }
        cancelContainers.clear();
      }
      if (rmCallbackHandler.getAllocatedWorkerContainerNumber() > 0 && !startAllocatedContainer) {
        startAllocatedContainer = true;
        startAllocatedTimeStamp = System.currentTimeMillis();
      }
      if (startAllocatedContainer && (System.currentTimeMillis() - startAllocatedTimeStamp) > conf.getInt(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS, YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS)) {
        String failMessage = "Container waiting except the allocated expiry time. Maybe the Cluster resource not satisfied the user necessary. Please resubmit !";
        LOG.info(failMessage);
        this.appendMessage("Unregister  Application", true);
        unregisterApp(FinalApplicationStatus.FAILED, failMessage);
        return false;
      }
      Utilities.sleep(allocateInterval);
    }
    acquiredPsContainers = rmCallbackHandler.getAcquiredPsContainer();
    acquiredWorkerContainers = rmCallbackHandler.getAcquiredWorkerContainer();
    int totalNumAllocatedWorkers = rmCallbackHandler.getAllocatedWorkerContainerNumber();
    if(totalNumAllocatedWorkers > workerNum) {
      while(acquiredWorkerContainers.size() > workerNum) {
        Container releaseContainer = acquiredWorkerContainers.remove(0);
        amrmAsync.releaseAssignedContainer(releaseContainer.getId());
        LOG.info("Release container " + releaseContainer.getId().toString());
      }
    }
    LOG.info("Total " + acquiredWorkerContainers.size() + " worker containers has allocated.");
    for (int i = 0; i < psNum; i++) {
      amrmAsync.removeContainerRequest(psContainerRequest);
    }
    for (int i = 0; i < workerNum; i++) {
      amrmAsync.removeContainerRequest(workerContainerRequest);
    }
    if(conf.getBoolean(HboxConfiguration.HBOX_HOST_LOCAL_ENABLE, HboxConfiguration.DEFAULT_HBOX_HOST_LOCAL_ENABLE)) {
      containerHostnames = new HashSet<>();
      if(acquiredPsContainers.size() > 0) {
        for(Container container: acquiredPsContainers) {
          containerHostnames.add(container.getNodeId().getHost());
        }
      }
      if(acquiredWorkerContainers.size() > 0) {
        for(Container container: acquiredWorkerContainers) {
          containerHostnames.add(container.getNodeId().getHost());
        }
      }
      LOG.info("host local enable is true, host list is: " + containerHostnames.toString());
    }

    //launch dist mxnet scheduler
    if(hboxAppType.equals("MXNET") && !singleMx) {
      LOG.info("Seting environments for the mxnet scheduler");
      dmlcPsRootUri = applicationMasterHostname;
      Socket schedulerReservedSocket = new Socket();
      try {
        Utilities.getReservePort(schedulerReservedSocket, InetAddress.getByName(applicationMasterHostname).getHostAddress(), reservePortBegin, reservePortEnd);
      } catch (IOException e) {
        LOG.error("Can not get available port");
      }
      dmlcPsRootPort = schedulerReservedSocket.getLocalPort();

      List<String> schedulerEnv = new ArrayList<>(20);
      Map<String, String> userEnv = new HashMap<>();
      if (conf.get(HboxConfiguration.HBOX_CONTAINER_ENV) != null) {
        String[] env = StringUtils.split(conf.get(HboxConfiguration.HBOX_CONTAINER_ENV), "|");
        for (String envPair : env) {
          String[] userEnvPair = StringUtils.split(envPair, "=");
          if (userEnvPair.length != 2) {
            LOG.error(envPair + " is not correct");
          } else {
            schedulerEnv.add(envPair);
            userEnv.put(userEnvPair[0], userEnvPair[1]);
          }
        }
      }
      if (userEnv.containsKey("PATH")) {
        schedulerEnv.add("PATH=" + userEnv.get("PATH") + System.getProperty("path.separator") + System.getenv("PATH"));
      } else {
        schedulerEnv.add("PATH=" + System.getenv("PATH"));
      }
      schedulerEnv.add("JAVA_HOME=" + System.getenv("JAVA_HOME"));
      schedulerEnv.add("HADOOP_HOME=" + System.getenv("HADOOP_HOME"));
      schedulerEnv.add("HADOOP_HDFS_HOME=" + System.getenv("HADOOP_HDFS_HOME"));
      if (userEnv.containsKey("LD_LIBRARY_PATH")) {
        schedulerEnv.add("LD_LIBRARY_PATH=" + "./:" + userEnv.get("LD_LIBRARY_PATH") + System.getProperty("path.separator") + System.getenv("LD_LIBRARY_PATH") + ":" + System.getenv("JAVA_HOME") +
            "/jre/lib/amd64/server:" + System.getenv("HADOOP_HOME") + "/lib/native");
      } else {
        schedulerEnv.add("LD_LIBRARY_PATH=" + "./:" + System.getenv("LD_LIBRARY_PATH") + ":" + System.getenv("JAVA_HOME") +
            "/jre/lib/amd64/server:" + System.getenv("HADOOP_HOME") + "/lib/native");
      }
      if (userEnv.containsKey("CLASSPATH")) {
        schedulerEnv.add("CLASSPATH=" + "./:" + userEnv.get("CLASSPATH") + System.getProperty("path.separator") + System.getenv("CLASSPATH") + ":" + System.getProperty("java.class.path"));
      } else {
        schedulerEnv.add("CLASSPATH=" + "./:" + System.getenv("CLASSPATH") + ":" + System.getProperty("java.class.path"));
      }
      schedulerEnv.add("DMLC_ROLE=scheduler");
      schedulerEnv.add("DMLC_PS_ROOT_URI=" + dmlcPsRootUri);
      schedulerEnv.add("DMLC_PS_ROOT_PORT=" + dmlcPsRootPort);
      schedulerEnv.add(HboxConstants.Environment.HBOX_DMLC_WORKER_NUM.toString() + "=" + workerNum);
      schedulerEnv.add(HboxConstants.Environment.HBOX_DMLC_SERVER_NUM.toString() + "=" + psNum);
      schedulerEnv.add("PYTHONUNBUFFERED=1");

      LOG.info("Executing command:" + hboxCommand);
      LOG.info("DMLC_PS_ROOT_URI is " + dmlcPsRootUri);
      LOG.info("DMLC_PS_ROOT_PORT is " + dmlcPsRootPort);
      LOG.info(HboxConstants.Environment.HBOX_DMLC_WORKER_NUM.toString() + "=" + workerNum);
      LOG.info(HboxConstants.Environment.HBOX_DMLC_SERVER_NUM.toString() + "=" + psNum);

      try {
        Runtime rt = Runtime.getRuntime();
        schedulerReservedSocket.close();
        final Process mxnetSchedulerProcess = rt.exec(hboxCommand, schedulerEnv.toArray(new String[schedulerEnv.size()]));
        LOG.info("Starting thread to redirect stdout of mxnet scheduler process");
        Thread mxnetSchedulerRedirectThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              BufferedReader reader;
              reader = new BufferedReader(new InputStreamReader(mxnetSchedulerProcess.getInputStream()));
              String mxnetSchedulerStdoutLog;
              while ((mxnetSchedulerStdoutLog = reader.readLine()) != null) {
                LOG.debug(mxnetSchedulerStdoutLog);
                if (conf.getBoolean(HboxConfiguration.HBOX_CONTAINER_RUNNING_LOG_ENABLE, HboxConfiguration.DEFAULT_HBOX_CONTAINER_RUNNING_LOG_ENABLE)) {
                  amContainerStdOut.append(mxnetSchedulerStdoutLog);
                }
              }
            } catch (Exception e) {
              LOG.warn("Exception in thread mxnetSchedulerRedirectThread");
              e.printStackTrace();
            }
          }
        });
        mxnetSchedulerRedirectThread.start();

        LOG.info("Starting thread to redirect stderr of mxnet scheduler process");
        Thread boardStderrRedirectThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              BufferedReader reader;
              reader = new BufferedReader(new InputStreamReader(mxnetSchedulerProcess.getErrorStream()));
              String mxnetSchedulerStderrLog;
              while ((mxnetSchedulerStderrLog = reader.readLine()) != null) {
                LOG.info(mxnetSchedulerStderrLog);
                if (conf.getBoolean(HboxConfiguration.HBOX_CONTAINER_RUNNING_LOG_ENABLE, HboxConfiguration.DEFAULT_HBOX_CONTAINER_RUNNING_LOG_ENABLE)) {
                  amContainerStdErr.append(mxnetSchedulerStderrLog);
                }
              }
            } catch (Exception e) {
              LOG.warn("Error in thread mxnetSchedulerStderrRedirectThread");
              e.printStackTrace();
            }
          }
        });
        boardStderrRedirectThread.start();
      } catch (Exception e ) {
        LOG.info("start mxnet scheduler error " + e);
      }

    }

    //launch dist xgboost scheduler
    if(hboxAppType.equals("DISTXGBOOST")) {
      LOG.info("Seting environments for the dist xgboost scheduler");
      dmlcTrackerUri = applicationMasterHostname;
      Socket schedulerReservedSocket = new Socket();
      try {
        Utilities.getReservePort(schedulerReservedSocket, InetAddress.getByName(applicationMasterHostname).getHostAddress(), reservePortBegin, reservePortEnd);
      } catch (IOException e) {
        LOG.error("Can not get available port");
      }
      dmlcTrackerPort = schedulerReservedSocket.getLocalPort();

      List<String> schedulerEnv = new ArrayList<>(20);
      Map<String, String> userEnv = new HashMap<>();
      if (conf.get(HboxConfiguration.HBOX_CONTAINER_ENV) != null) {
        String[] env = StringUtils.split(conf.get(HboxConfiguration.HBOX_CONTAINER_ENV), "|");
        for (String envPair : env) {
          String[] userEnvPair = StringUtils.split(envPair, "=");
          if (userEnvPair.length != 2) {
            LOG.error(envPair + " is not correct");
          } else {
            schedulerEnv.add(envPair);
            userEnv.put(userEnvPair[0], userEnvPair[1]);
          }
        }
      }
      if (userEnv.containsKey("PATH")) {
        schedulerEnv.add("PATH=" + userEnv.get("PATH") + System.getProperty("path.separator") + System.getenv("PATH"));
      } else {
        schedulerEnv.add("PATH=" + System.getenv("PATH"));
      }
      schedulerEnv.add("JAVA_HOME=" + System.getenv("JAVA_HOME"));
      schedulerEnv.add("HADOOP_HOME=" + System.getenv("HADOOP_HOME"));
      schedulerEnv.add("HADOOP_HDFS_HOME=" + System.getenv("HADOOP_HDFS_HOME"));
      if (userEnv.containsKey("LD_LIBRARY_PATH")) {
        schedulerEnv.add("LD_LIBRARY_PATH=" + "./:" + userEnv.get("LD_LIBRARY_PATH") + System.getProperty("path.separator") + System.getenv("LD_LIBRARY_PATH") + ":" + System.getenv("JAVA_HOME") +
            "/jre/lib/amd64/server:" + System.getenv("HADOOP_HOME") + "/lib/native");
      } else {
        schedulerEnv.add("LD_LIBRARY_PATH=" + "./:" + System.getenv("LD_LIBRARY_PATH") + ":" + System.getenv("JAVA_HOME") +
            "/jre/lib/amd64/server:" + System.getenv("HADOOP_HOME") + "/lib/native");
      }
      if (userEnv.containsKey("CLASSPATH")) {
        schedulerEnv.add("CLASSPATH=" + "./:" + userEnv.get("CLASSPATH") + System.getProperty("path.separator") + System.getenv("CLASSPATH") + ":" + System.getProperty("java.class.path"));
      } else {
        schedulerEnv.add("CLASSPATH=" + "./:" + System.getenv("CLASSPATH") + ":" + System.getProperty("java.class.path"));
      }
      schedulerEnv.add("PYTHONUNBUFFERED=1");

      String distXgboostSchedulerCmd = "python xgboost/self-define/rabitTracker.py --num-workers=" + workerNum
              + " --host-ip=" + dmlcTrackerUri + " --port=" + dmlcTrackerPort;
      LOG.info("Dist xgboost scheduler executing command:" + distXgboostSchedulerCmd);
      LOG.info("DMLC_TRACKER_URI is " + dmlcTrackerUri);
      LOG.info("DMLC_TRACKER_PORT is " + dmlcTrackerPort);
      LOG.info("DMLC_NUM_WORKER=" + workerNum);

      try {
        Runtime rt = Runtime.getRuntime();
        schedulerReservedSocket.close();
        final Process xgboostSchedulerProcess = rt.exec(distXgboostSchedulerCmd, schedulerEnv.toArray(new String[schedulerEnv.size()]));
        LOG.info("Starting thread to redirect stdout of xgboost scheduler process");
        Thread xgboostSchedulerRedirectThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              BufferedReader reader;
              reader = new BufferedReader(new InputStreamReader(xgboostSchedulerProcess.getInputStream()));
              String xgboostSchedulerStdoutLog;
              while ((xgboostSchedulerStdoutLog = reader.readLine()) != null) {
                LOG.info(xgboostSchedulerStdoutLog);
                appendMessage(xgboostSchedulerStdoutLog, false);
                if (conf.getBoolean(HboxConfiguration.HBOX_CONTAINER_RUNNING_LOG_ENABLE, HboxConfiguration.DEFAULT_HBOX_CONTAINER_RUNNING_LOG_ENABLE)) {
                  amContainerStdOut.append(xgboostSchedulerStdoutLog);
                }
              }
            } catch (Exception e) {
              LOG.warn("Exception in thread xgboostSchedulerRedirectThread");
              e.printStackTrace();
            }
          }
        });
        xgboostSchedulerRedirectThread.start();

        LOG.info("Starting thread to redirect stderr of xgboost scheduler process");
        Thread xgboostSchedulerStderrRedirectThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              BufferedReader reader;
              reader = new BufferedReader(new InputStreamReader(xgboostSchedulerProcess.getErrorStream()));
              String xgboostSchedulerStderrLog;
              while ((xgboostSchedulerStderrLog = reader.readLine()) != null) {
                LOG.info(xgboostSchedulerStderrLog);
                appendMessage(xgboostSchedulerStderrLog, false);
                if (conf.getBoolean(HboxConfiguration.HBOX_CONTAINER_RUNNING_LOG_ENABLE, HboxConfiguration.DEFAULT_HBOX_CONTAINER_RUNNING_LOG_ENABLE)) {
                  amContainerStdErr.append(xgboostSchedulerStderrLog);
                }
              }
            } catch (Exception e) {
              LOG.warn("Error in thread xgboostSchedulerStderrRedirectThread");
              e.printStackTrace();
            }
          }
        });
        xgboostSchedulerStderrRedirectThread.start();
      } catch (Exception e ) {
        LOG.info("start xgboost scheduler error " + e);
      }

    }

    // launch xflow scheduler
    if (("XFLOW").equals(hboxAppType)) {
      LOG.info("Setting environments for the xflow scheduler");
      InetAddress address = null;
      try {
        address = InetAddress.getByName(applicationMasterHostname);
        dmlcPsRootUri = address.getHostAddress();
      } catch (UnknownHostException e) {
        LOG.info("acquire host ip failed " + e);
      }
      Socket schedulerReservedSocket = new Socket();
      try {
        Utilities.getReservePort(schedulerReservedSocket, InetAddress.getByName(applicationMasterHostname).getHostAddress(), reservePortBegin, reservePortEnd);
      } catch (IOException e) {
        LOG.error("Can not get available port");
      }
      dmlcPsRootPort = schedulerReservedSocket.getLocalPort();


      List<String> schedulerEnv = new ArrayList<>(20);
      Map<String, String> userEnv = new HashMap<>();
      if (conf.get(HboxConfiguration.HBOX_CONTAINER_ENV) != null) {
        String[] env = StringUtils.split(conf.get(HboxConfiguration.HBOX_CONTAINER_ENV), "|");
        for (String envPair : env) {
          String[] userEnvPair = StringUtils.split(envPair, "=");
          if (userEnvPair.length != 2) {
            LOG.error(envPair + " is not correct");
          } else {
            schedulerEnv.add(envPair);
            userEnv.put(userEnvPair[0], userEnvPair[1]);
          }
        }
      }
      if (userEnv.containsKey("PATH")) {
        schedulerEnv.add("PATH=" + userEnv.get("PATH") + System.getProperty("path.separator") + System.getenv("PATH"));
      } else {
        schedulerEnv.add("PATH=" + System.getenv("PATH"));
      }
      schedulerEnv.add("JAVA_HOME=" + System.getenv("JAVA_HOME"));
      schedulerEnv.add("HADOOP_HOME=" + System.getenv("HADOOP_HOME"));
      schedulerEnv.add("HADOOP_HDFS_HOME=" + System.getenv("HADOOP_HDFS_HOME"));
      if (userEnv.containsKey("LD_LIBRARY_PATH")) {
        schedulerEnv.add("LD_LIBRARY_PATH=" + "./:" + userEnv.get("LD_LIBRARY_PATH") + System.getProperty("path.separator") + System.getenv("LD_LIBRARY_PATH") + ":" + System.getenv("JAVA_HOME") +
            "/jre/lib/amd64/server:" + System.getenv("HADOOP_HOME") + "/lib/native");
      } else {
        schedulerEnv.add("LD_LIBRARY_PATH=" + "./:" + System.getenv("LD_LIBRARY_PATH") + ":" + System.getenv("JAVA_HOME") +
            "/jre/lib/amd64/server:" + System.getenv("HADOOP_HOME") + "/lib/native");
      }
      if (userEnv.containsKey("CLASSPATH")) {
        schedulerEnv.add("CLASSPATH=" + "./:" + userEnv.get("CLASSPATH") + System.getProperty("path.separator") + System.getenv("CLASSPATH") + ":" + System.getProperty("java.class.path"));
      } else {
        schedulerEnv.add("CLASSPATH=" + "./:" + System.getenv("CLASSPATH") + ":" + System.getProperty("java.class.path"));
      }
      schedulerEnv.add("DMLC_ROLE=scheduler");
      schedulerEnv.add("DMLC_PS_ROOT_URI=" + dmlcPsRootUri);
      schedulerEnv.add("DMLC_PS_ROOT_PORT=" + dmlcPsRootPort);
      schedulerEnv.add(HboxConstants.Environment.HBOX_DMLC_WORKER_NUM.toString() + "=" + workerNum);
      schedulerEnv.add(HboxConstants.Environment.HBOX_DMLC_SERVER_NUM.toString() + "=" + psNum);
      schedulerEnv.add("PYTHONUNBUFFERED=1");

      LOG.info("Executing command:" + hboxCommand);
      LOG.info("DMLC_PS_ROOT_URI is " + dmlcPsRootUri);
      LOG.info("DMLC_PS_ROOT_PORT is " + dmlcPsRootPort);
      LOG.info(HboxConstants.Environment.HBOX_DMLC_WORKER_NUM.toString() + "=" + workerNum);
      LOG.info(HboxConstants.Environment.HBOX_DMLC_SERVER_NUM.toString() + "=" + psNum);

      try {
        Runtime rt = Runtime.getRuntime();
        schedulerReservedSocket.close();
        final Process xflowSchedulerProcess = rt.exec(hboxCommand, schedulerEnv.toArray(new String[schedulerEnv.size()]));
        LOG.info("Starting thread to redirect stdout of xflow scheduler process");
        Thread xflowSchedulerRedirectThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              BufferedReader reader;
              reader = new BufferedReader(new InputStreamReader(xflowSchedulerProcess.getInputStream()));
              String xflowSchedulerStdoutLog;
              while ((xflowSchedulerStdoutLog = reader.readLine()) != null) {
                LOG.info(xflowSchedulerStdoutLog);
                if (conf.getBoolean(HboxConfiguration.HBOX_CONTAINER_RUNNING_LOG_ENABLE, HboxConfiguration.DEFAULT_HBOX_CONTAINER_RUNNING_LOG_ENABLE)) {
                  amContainerStdOut.append(xflowSchedulerStdoutLog);
                }
              }
            } catch (Exception e) {
              LOG.warn("Exception in thread xflowSchedulerRedirectThread");
              e.printStackTrace();
            }
          }
        });
        xflowSchedulerRedirectThread.start();

        LOG.info("Starting thread to redirect stderr of xflow scheduler process");
        Thread xflowSchedulerStderrRedirectThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              BufferedReader reader;
              reader = new BufferedReader(new InputStreamReader(xflowSchedulerProcess.getErrorStream()));
              String xflowSchedulerStderrLog;
              while ((xflowSchedulerStderrLog = reader.readLine()) != null) {
                LOG.info(xflowSchedulerStderrLog);
                if (conf.getBoolean(HboxConfiguration.HBOX_CONTAINER_RUNNING_LOG_ENABLE, HboxConfiguration.DEFAULT_HBOX_CONTAINER_RUNNING_LOG_ENABLE)) {
                  amContainerStdErr.append(xflowSchedulerStderrLog);
                }
              }
            } catch (Exception e) {
              LOG.warn("Error in thread xflowSchedulerStderrRedirectThread");
              e.printStackTrace();
            }
          }
        });
        xflowSchedulerStderrRedirectThread.start();
      } catch (Exception e) {
        LOG.info("start xflow scheduler error " + e);
      }

    }

    //launch mpi exec process
    if(hboxAppType.equals("MPI") || hboxAppType.equals("HOROVOD")) {
      if(hboxAppType.equals("MPI"))
        launchMpiExec();
      else launchHorovodExec();
      mpiExitCode = -1;
      while (mpiContainerCommand == null) {
        Utilities.sleep(statusUpdateInterval);
        try {
          mpiExitCode = mpiExecProcess.exitValue();
        } catch (IllegalThreadStateException e) {
          LOG.debug(hboxAppType.toLowerCase() + " exec process is running");
        }
        if (mpiExitCode != -1) {
          appendMessage(new Message(LogType.STDERR, hboxAppType.toLowerCase() + " exec exit with code " + mpiExitCode));
          throw new HboxExecException(hboxAppType.toLowerCase() + "exec exit with code " + mpiExitCode);
        }
      }
    }

    if(conf.getBoolean(HboxConfiguration.HBOX_INPUT_STREAM, HboxConfiguration.DEFAULT_HBOX_INPUT_STREAM) ||
         conf.get(HboxConfiguration.HBOX_INPUT_STRATEGY, HboxConfiguration.DEFAULT_HBOX_INPUT_STRATEGY).equals("STREAM")) {
      if (inputFileSplits != null) {
        allocateInputStreamSplits();
      } else {
        LOG.info("Don't have the input splits to allocated");
      }
    } else {
      if(conf.get(HboxConfiguration.HBOX_INPUT_STRATEGY, HboxConfiguration.DEFAULT_HBOX_INPUT_STRATEGY).equals("PLACEHOLDER")
              && conf.getBoolean(HboxConfiguration.HBOX_PLACEHOLDER_WHOLE_ENABLE, HboxConfiguration.DEFAULT_HBOX_PLACEHOLDER_WHOLE_ENABLE)) {
        allocateWholeInput();
      } else {
        if(conf.getBoolean(HboxConfiguration.HBOX_TF_INPUT_PS_ENABLE, HboxConfiguration.DEFAULT_HBOX_TF_INPUT_PS_ENABLE)) {
          allocateInputSplitsInlcudePs();
        } else {
          allocateInputSplits();
        }
      }
    }
    buildOutputLocations();
    buildContainerLocalResource();
    Map<String, String> workerContainerEnv = buildContainerEnv(HboxConstants.WORKER);
    Map<String, String> psContainerEnv = buildContainerEnv(HboxConstants.PS);
    List<String> workerContainerLaunchcommands = buildContainerLaunchCommand(workerMemory);
    List<String> psContainerLaunchcommands = buildContainerLaunchCommand(psMemory);

    LOG.info("Launching containers");
    int index = 0;
    for (Container container : acquiredPsContainers) {
      LOG.info("Launching ps container " + container.getId()
          + " on " + container.getNodeId().getHost() + ":" + container.getNodeId().getPort());

      //TODO launch container in special thread take with fault-tolerant
      launchContainer(containerLocalResource, psContainerEnv,
          psContainerLaunchcommands,container, index ++);
      containerListener.registerContainer(new HboxContainerId(container.getId()), HboxConstants.PS);
    }
    if(hboxAppType.equals("MPI") || hboxAppType.equals("HOROVOD")) {
      index = 1;
    } else {
      index = 0;
    }
    for (Container container : acquiredWorkerContainers) {
      LOG.info("Launching worker container " + container.getId()
          + " on " + container.getNodeId().getHost() + ":" + container.getNodeId().getPort());

      //TODO launch container in special thread take with fault-tolerant
      launchContainer(containerLocalResource, workerContainerEnv,
          workerContainerLaunchcommands,container, index ++);
      containerListener.registerContainer(new HboxContainerId(container.getId()), HboxConstants.WORKER);
      if (conf.getBoolean(HboxConfiguration.HBOX_TF_EVALUATOR, HboxConfiguration.DEFAULT_HBOX_TF_EVALUATOR) && index == workerNum) {
        tfEvaluatorContainerId = container.getId().toString();
      }
    }

    if(hboxAppType.equals("MPI") || hboxAppType.equals("HOROVOD")) {
      while (!containerListener.isAllContainerStarted()) {
        Utilities.sleep(statusUpdateInterval);
      }
      this.appendMessage(new Message(LogType.STDERR, "All containers are launched successfully"));
      while (mpiExitCode == -1) {
        Utilities.sleep(statusUpdateInterval);
        try {
          mpiExitCode = mpiExecProcess.exitValue();
        } catch (IllegalThreadStateException e) {
          LOG.debug(hboxAppType.toLowerCase() + " exec process is running");
        }
      }
      appendMessage(new Message(LogType.STDERR, "finish mpiexec with code " + mpiExitCode));
    }

    String diagnostics = "";
    boolean finalSuccess;

    if (this.applicationContext.getOutputs().size() > 0) {
      final Thread saveInnerModelMonitor = new Thread(new Runnable() {
        @Override
        public void run() {
          while (true) {
            try {
              Boolean startSaved = applicationContext.getStartSavingStatus();
              containerListener.setSaveInnerModel(startSaved);
              if (savingInterval > 0 && !startSaved) {
                if ((containerListener.interResultTimeStamp() < 0 && containerListener.allContainerStartTime() > 0 && (System.currentTimeMillis() - containerListener.allContainerStartTime()) > savingInterval) ||
                    ((System.currentTimeMillis() - containerListener.interResultTimeStamp()) > savingInterval)){
                  applicationContext.startSavingModelStatus(true);
                  containerListener.setSaveInnerModel(true);
                  startSaved = true;
                }
              }
              while (startSaved) {
                if (containerListener.interResultCompletedNum(containerListener.interResultTimeStamp())
                    == containerListener.getInnerSavingContainerNum()) {
                  lastSavingStatus = true;
                  if (!savingModelList.contains(containerListener.interResultTimeStamp())) {
                    savingModelList.add(containerListener.interResultTimeStamp());
                  }
                  break;
                }
                Utilities.sleep(conf.getInt(HboxConfiguration.HBOX_CONTAINER_HEARTBEAT_INTERVAL, HboxConfiguration.DEFAULT_HBOX_CONTAINER_HEARTBEAT_INTERVAL));
              }
            } catch (Exception e) {
              LOG.info("Monitor the InnerModel saving error: " + e);
            }
          }
        }
      });
      saveInnerModelMonitor.start();
    }

    containerStarted = true;
    try {
      boolean flag = true;
      boolean digitsFlag = true;
      if(!hboxAppType.equals("MPI") && !hboxAppType.equals("HOROVOD")) {
        LOG.info("Waiting for train completed");
        Map<HboxContainerId, HboxContainerStatus> lastWorkerContainerStatus = new ConcurrentHashMap<>();
        Map<HboxContainerId, HboxContainerStatus> lastPsContainerStatus = new ConcurrentHashMap<>();
        while (!containerListener.isTrainCompleted()) {
          //report progress to client
          if(conf.getBoolean(HboxConfiguration.HBOX_REPORT_CONTAINER_STATUS, HboxConfiguration.DEFAULT_HBOX_REPORT_CONTAINER_STATUS) && !hboxAppType.equals("MPI") && !hboxAppType.equals("HOROVOD")) {
            List<Container> workerContainersStatus = applicationContext.getWorkerContainers();
            List<Container> psContainersStatus = applicationContext.getPsContainers();
            for(Container container : workerContainersStatus) {
              if(!lastWorkerContainerStatus.containsKey(new HboxContainerId(container.getId()))) {
                lastWorkerContainerStatus.put(new HboxContainerId(container.getId()), HboxContainerStatus.STARTED);
              }
              if(!applicationContext.getContainerStatus(new HboxContainerId(container.getId())).equals(lastWorkerContainerStatus.get(new HboxContainerId(container.getId())))) {
                this.appendMessage("container " + container.getId().toString() + " status is " + applicationContext.getContainerStatus(new HboxContainerId(container.getId())), false);
                lastWorkerContainerStatus.put(new HboxContainerId(container.getId()),applicationContext.getContainerStatus(new HboxContainerId(container.getId())));
              }
            }
            for(Container container : psContainersStatus) {
              if(!lastPsContainerStatus.containsKey(new HboxContainerId(container.getId()))) {
                lastPsContainerStatus.put(new HboxContainerId(container.getId()), HboxContainerStatus.STARTED);
              }
              if(!applicationContext.getContainerStatus(new HboxContainerId(container.getId())).equals(lastPsContainerStatus.get(new HboxContainerId(container.getId())))) {
                this.appendMessage("container " + container.getId().toString() + " status is " + applicationContext.getContainerStatus(new HboxContainerId(container.getId())), false);
                lastPsContainerStatus.put(new HboxContainerId(container.getId()),applicationContext.getContainerStatus(new HboxContainerId(container.getId())));
              }
            }
          }

          String containerType = conf.get(HboxConfiguration.CONTAINER_EXECUTOR_TYPE,
                  HboxConfiguration.DEFAULT_CONTAINER_EXECUTOR_TYPE).toUpperCase();
          List<Container> workerContainers = applicationContext.getWorkerContainers();
          List<Container> psContainers = applicationContext.getPsContainers();
          if(hboxAppType.equals("VPC") || hboxAppType.equals("DIGITS") || containerType.equals("DOCKER")) {
            if(flag) {
              Map<HboxContainerId, String> vpcCommandAndPasswdMap = applicationContext.getVPCCommandAndPasswdMap();
              if( vpcCommandAndPasswdMap.size() == (workerNum + psNum)) {
                for(Container container : workerContainers) {
                  String commandCombine = vpcCommandAndPasswdMap.get(new HboxContainerId(container.getId()));
                  String[] splits = commandCombine.split(":");
                  this.appendMessage("Received vpc login command and password from " + container.getId().toString(), true);
                  this.appendMessage("Login command:ssh " + splits[0], true);
                  this.appendMessage("Password:" + splits[1], true);
                }
                for(Container container : psContainers) {
                  String commandCombine = vpcCommandAndPasswdMap.get(new HboxContainerId(container.getId()));
                  String[] splits = commandCombine.split(":");
                  this.appendMessage("Received vpc login command and password from " + container.getId().toString(), true);
                  this.appendMessage("Login command:ssh " + splits[0], true);
                  this.appendMessage("Password:" + splits[1], true);
                }
                flag = false;
              } else {
                this.appendMessage("Waiting for vpc login command and password...", false);
              }
            }
            if(hboxAppType.equals("DIGITS")) {
              if(digitsFlag) {
                Map<HboxContainerId, String> digitsUrlMap = applicationContext.getDigitsUrlMap();
                if( digitsUrlMap.size() == (workerNum + psNum)) {
                  for(Container container : workerContainers) {
                    String url = digitsUrlMap.get(new HboxContainerId(container.getId()));
                    this.appendMessage("Received digits server url from " + container.getId().toString(), true);
                    this.appendMessage("digits server url: " + url, true);
                  }
                  for(Container container : psContainers) {
                    String url = digitsUrlMap.get(new HboxContainerId(container.getId()));
                    this.appendMessage("Received digits server url from " + container.getId().toString(), true);
                    this.appendMessage("digits server url: " + url, true);
                  }
                  digitsFlag = false;
                } else {
                  this.appendMessage("Waiting for digits server url...", false);
                }
              }
            }
          }

          // check the gpu device assigned whether correct
          if (conf.getInt(HboxConfiguration.HBOX_PS_GPU, HboxConfiguration.DEFAULT_HBOX_PS_GPU) > 0) {
            for (Container container : psContainers) {
              HboxContainerId containerId = new HboxContainerId(container.getId());
              if (applicationContext.getContainerGPUDevice(containerId) != null) {
                String gpus = applicationContext.getContainerGPUDevice(containerId);
                if (gpus.equals("") || gpus.equals(null) || gpus.split(",").length != conf.getInt(HboxConfiguration.HBOX_PS_GPU, HboxConfiguration.DEFAULT_HBOX_PS_GPU))
                  throw new RuntimeException("ps container " + containerId.toString() + " is not assigned the correct gpu device. Now assigned info is " + gpus);
              }
            }
          }

          if (conf.getInt(HboxConfiguration.HBOX_WORKER_GPU, HboxConfiguration.DEFAULT_HBOX_WORKER_GPU) > 0) {
            for (Container container : workerContainers) {
              HboxContainerId containerId = new HboxContainerId(container.getId());
              if (applicationContext.getContainerGPUDevice(containerId) != null) {
                String gpus = applicationContext.getContainerGPUDevice(containerId);
                if (gpus.equals("") || gpus.equals(null) || gpus.split(",").length != conf.getInt(HboxConfiguration.HBOX_WORKER_GPU, HboxConfiguration.DEFAULT_HBOX_WORKER_GPU))
                  throw new RuntimeException("worker container " + containerId.toString() + " is not assigned the correct gpu device. Now assigned info is " + gpus);
              }
            }
          }

          Map<HboxContainerId, String> clientProgress = applicationContext.getReporterProgress();
          float total = 0.0f;
          for (Container container : workerContainers) {
            String progressLog = clientProgress.get(new HboxContainerId(container.getId()));
            if(progressLog != null && !progressLog.equals("")) {
              String[] progress = progressLog.toString().split(":");
              if(progress.length != 2) {
                this.appendMessage("progress log format error", false);
              } else {
                try {
                  Float percentProgress = Float.parseFloat(progress[1]);
                  if(percentProgress < 0.0 || percentProgress > 1.0) {
                    this.appendMessage("progress log format error", false);
                  } else {
                    total += Float.parseFloat(progress[1]);
                  }
                } catch (Exception e) {
                  this.appendMessage("progress log format error", false);
                }
              }
            }
          }
          if(total > 0.0f) {
            float finalProgress = total / workerContainers.size();
            DecimalFormat df = new DecimalFormat("0.00");
            df.setRoundingMode(RoundingMode.HALF_UP);
            //this.appendMessage("reporter progress:" + Float.toString(finalProgress*100) + "%", false);
            this.appendMessage("reporter progress:" + df.format(finalProgress*100) + "%", false);
            rmCallbackHandler.setProgress(finalProgress);
          }

          Utilities.sleep(statusUpdateInterval);
        }
        LOG.info("Train completed");
        containerListener.setTrainFinished();

        if("TENSORFLOW".equals(hboxAppType) && !single) {
          LOG.info("Waiting all ps contianers completed");
          while (!containerListener.isAllPsContainersFinished()) {
            Utilities.sleep(statusUpdateInterval);
          }
          LOG.info("All ps containers completed");
        }

        if("MXNET".equals(hboxAppType) && !singleMx) {
          LOG.info("Waiting all server contianers completed");
          while (!containerListener.isAllPsContainersFinished()) {
            Utilities.sleep(statusUpdateInterval);
          }
          LOG.info("All server containers completed");
        }

        if("DISTLIGHTLDA".equals(hboxAppType)) {
          LOG.info("Waiting all ps contianers completed");
          while (!containerListener.isAllPsContainersFinished()) {
            Utilities.sleep(statusUpdateInterval);
          }
          LOG.info("All ps containers completed");
        }

        if("XFLOW".equals(hboxAppType)) {
          LOG.info("Waiting all ps containers completed");
          while (!containerListener.isAllPsContainersFinished()) {
            Utilities.sleep(statusUpdateInterval);
          }
          LOG.info("All ps containers completed");
        }

        finalSuccess = containerListener.isAllWorkerContainersSucceeded();
      } else {
        containerListener.setAMFinished();
        LOG.info("Waiting all containers completed");
        finalSuccess = mpiExitCode == 0;
        while (!containerListener.isTrainCompleted()) {
          Utilities.sleep(statusUpdateInterval);
        }
        LOG.info("All containers completed");
      }
      if (finalSuccess) {
        if((conf.getBoolean(HboxConfiguration.HBOX_OUTPUT_STREAM, HboxConfiguration.DEFAULT_HBOX_OUTPUT_STREAM)
             || conf.get(HboxConfiguration.HBOX_OUTPUT_STRATEGY, HboxConfiguration.DEFAULT_HBOX_OUTPUT_STRATEGY).equals("STREAM")) && outputInfos.size() > 0) {
          LOG.info("HBOX_OUTPUT_STRATEGY is STREAM, AM handling the final result...");
          FileSystem fs = new Path(outputInfos.get(0).getDfsLocation()).getFileSystem(conf);
          Map<HboxContainerId, String>  mapPath = applicationContext.getMapedTaskID();
          for (Container finishedContainer : acquiredWorkerContainers) {
            String taskID = mapPath.get(new HboxContainerId(finishedContainer.getId()));
            Path tmpResultPath = new Path(outputInfos.get(0).getDfsLocation() + "/_temporary/" + finishedContainer.getId().toString()
                    + "/_temporary/0/_temporary/" + taskID);
            LOG.info("tmpResultPath is " + tmpResultPath.toString());
            Path finalResultPath = new Path(outputInfos.get(0).getDfsLocation() + "/" + finishedContainer.getId().toString());
            LOG.info("finalResultPath is " + finalResultPath.toString());
            if (fs.exists(tmpResultPath)) {
              LOG.info("Move from " + tmpResultPath.toString() + " to " + finalResultPath.toString());
              fs.rename(tmpResultPath, finalResultPath);
            }
          }
          Path tmpPath = new Path(outputInfos.get(0).getDfsLocation() + "/_temporary/");
          if (fs.exists(tmpPath)) {
            fs.delete(tmpPath, true);
          }
          fs.createNewFile(new Path(outputInfos.get(0).getDfsLocation() + "/_SUCCESS"));
        } else {
          for (OutputInfo outputInfo : outputInfos) {
            FileSystem fs = new Path(outputInfo.getDfsLocation()).getFileSystem(conf);
            Path finalResultPath = new Path(outputInfo.getDfsLocation());
            for (Container finishedContainer : acquiredWorkerContainers) {
              Path tmpResultPath = new Path(outputInfo.getDfsLocation() + "/_temporary/" + finishedContainer.getId().toString());
              if(workerNum == 1 && !conf.getBoolean(HboxConfiguration.HBOX_CREATE_CONTAINERID_DIR, HboxConfiguration.DEFAULT_HBOX_CREATE_CONTAINERID_DIR)) {
                tmpResultPath = new Path(outputInfo.getDfsLocation() + "/_temporary/" + outputInfo.getLocalLocation());
              }
              if (fs.exists(tmpResultPath)) {
                LOG.info("Move from " + tmpResultPath.toString() + " to " + finalResultPath);
                fs.rename(tmpResultPath, finalResultPath);
              }
            }
            if(psNum > 0 && (hboxAppType.equals("DISTLIGHTLDA") || hboxAppType.equals("TENSORFLOW"))) {
              for (Container finishedContainer : acquiredPsContainers) {
                Path tmpResultPath = new Path(outputInfo.getDfsLocation() + "/_temporary/" + finishedContainer.getId().toString());
                if (fs.exists(tmpResultPath)) {
                  LOG.info("Move from " + tmpResultPath.toString() + " to " + finalResultPath);
                  fs.rename(tmpResultPath, finalResultPath);
                }
              }
            }
            Path tmpPath = new Path(outputInfo.getDfsLocation() + "/_temporary/");
            if (fs.exists(tmpPath)) {
              fs.delete(tmpPath, true);
            }
            fs.createNewFile(new Path(outputInfo.getDfsLocation() + "/_SUCCESS"));
          }
        }
      }
    } catch (Exception e) {
      finalSuccess = false;
      this.appendMessage("Some error occurs"
          + org.apache.hadoop.util.StringUtils.stringifyException(e), true);
      diagnostics = e.getMessage();
    }

    int appAttempts = conf.getInt(HboxConfiguration.HBOX_APP_MAX_ATTEMPTS, HboxConfiguration.DEFAULT_HBOX_APP_MAX_ATTEMPTS);
    if (appAttempts > conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)) {
      appAttempts = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    }

    if (!finalSuccess && applicationAttemptID.getAttemptId() < appAttempts) {
      Runtime.getRuntime().removeShutdownHook(cleanApplication);
      throw new RuntimeException("Application Failed, retry starting. Note that container memory auto scale");
    }

    this.appendMessage("Unregister  Application", true);
    unregisterApp(finalSuccess ? FinalApplicationStatus.SUCCEEDED
        : FinalApplicationStatus.FAILED, diagnostics);

    return finalSuccess;
  }

  /**
   * Internal class for running application class
   */
  private class RunningAppContext implements ApplicationContext {

    @Override
    public ApplicationId getApplicationID() {
      return applicationAttemptID.getApplicationId();
    }

    @Override
    public String getAppType(){
      return hboxAppType;
    }

    @Override
    public String getAppUser(){
      return userName;
    }

    @Override
    public int getWorkerNum() {
      if (tfEvaluator) {
        return workerNum - 1;
      } else {
        return workerNum;
      }
    }

    @Override
    public int getPsNum() {
      return psNum;
    }

    @Override
    public int getWorkerGcores() {
      return workerGCores;
    }

    @Override
    public int getPsGcores(){
      return psGCores;
    }

    @Override
    public int getWorkerMemory(){
      return workerMemory;
    }

    @Override
    public int getPsMemory(){
      return psMemory;
    }

    @Override
    public int getWorkerVCores(){
      return workerVCores;
    }

    @Override
    public int getPsVCores(){
      return psVCores;
    }

    @Override
    public List<Container> getWorkerContainers() {
      return acquiredWorkerContainers;
    }

    @Override
    public List<Container> getPsContainers() {
      return acquiredPsContainers;
    }

    @Override
    public HboxContainerStatus getContainerStatus(HboxContainerId containerId) {
      return containerListener.getContainerStatus(containerId);
    }

    @Override
    public String getContainerGPUDevice(HboxContainerId containerId) {
      return containerListener.getContainerGPUDevice(containerId);
    }

    @Override
    public LinkedBlockingQueue<Message> getMessageQueue() {
      return applicationMessageQueue;
    }

    @Override
    public List<InputInfo> getInputs(HboxContainerId containerId) {
      if (!containerId2InputInfo.containsKey(containerId)) {
        LOG.info("containerId2InputInfo not conains" + containerId.getContainerId());
        return new ArrayList<InputInfo>();
      }
      return containerId2InputInfo.get(containerId);
    }

    @Override
    public Map<String, InputInfo> getWholeInputs() {
      LOG.info("wholeFiles size is " + wholeFiles.size());
      return wholeFiles;
    }

    @Override
    public List<InputSplit> getStreamInputs(HboxContainerId containerId) {
      if (!containerId2InputSplit.containsKey(containerId)) {
        LOG.info("containerId2InputSplit not conains" + containerId.getContainerId());
        return new ArrayList<InputSplit>();
      }
      return containerId2InputSplit.get(containerId);
    }

    @Override
    public List<OutputInfo> getOutputs() {
      return outputInfos;
    }

    @Override
    public String getTensorBoardUrl() {
      return containerListener.getTensorboardUrl();
    }

    @Override
    public Map<HboxContainerId, String> getVPCCommandAndPasswdMap() {
      return  containerListener.getVPCCommandAndPasswdMap();
    }

    @Override
    public Map<HboxContainerId, String> getDigitsUrlMap() {
      return  containerListener.getDigitsUrlMap();
    }

    @Override
    public Map<HboxContainerId, String> getReporterProgress() {
      return containerListener.getReporterProgress();
    }

    @Override
    public Map<HboxContainerId, String> getContainersAppStartTime() {
      return containerListener.getContainersAppStartTime();
    }

    @Override
    public Map<HboxContainerId, String> getContainersAppFinishTime() {
      return containerListener.getContainersAppFinishTime();
    }

    @Override
    public Map<HboxContainerId, String> getMapedTaskID() {
      return containerListener.getMapedTaskID();
    }

    @Override
    public Map<HboxContainerId, ConcurrentHashMap<String, LinkedBlockingDeque<List<Long>>>> getContainersGpuMemMetrics() {
      return containerListener.getContainersGpuMemMetrics();
    }

    @Override
    public Map<HboxContainerId, ConcurrentHashMap<String, LinkedBlockingDeque<List<Long>>>> getContainersGpuUtilMetrics() {
      return containerListener.getContainersGpuUtilMetrics();
    }

    @Override
    public Map<HboxContainerId, ConcurrentHashMap<String, LinkedBlockingDeque<Object>>> getContainersCpuMetrics() {
      return containerListener.getContainersCpuMetrics();
    }

    @Override
    public Map<HboxContainerId, ConcurrentHashMap<String, List<Double>>> getContainersGpuUtilStatistics(){
      return containerListener.getContainersGpuUtilStatistics();
    }

    @Override
    public Map<HboxContainerId, ConcurrentHashMap<String, List<Double>>> getContainersGpuMemStatistics(){
      return containerListener.getContainersGpuMemStatistics();
    }

    @Override
    public Map<HboxContainerId, ConcurrentHashMap<String, List<Double>>> getContainersCpuStatistics(){
      return containerListener.getContainersCpuStatistics();
    }

    @Override
    public String getLastInterSavingPath() {
      Path interPath = new Path(conf.get(HboxConfiguration.HBOX_INTERREAULST_DIR, HboxConfiguration.DEFAULT_HBOX_INTERRESULT_DIR)
          + new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(containerListener.interResultTimeStamp()));
      return interPath.toString();
    }

    @Override
    public int getSavingModelStatus() {
      return containerListener.interResultCompletedNum(containerListener.interResultTimeStamp());
    }

    @Override
    public Boolean getStartSavingStatus() {
      return startSavingModel;
    }

    @Override
    public int getSavingModelTotalNum() {
      return containerListener.getInnerSavingContainerNum();
    }

    @Override
    public void startSavingModelStatus(Boolean flag) {
      LOG.info("current savingModelStatus is " + startSavingModel + ", the last savingStatus is " + lastSavingStatus);
      if (flag && !startSavingModel) {
        lastSavingStatus = false;
        startSavingModel = true;
      }
      if (!flag && startSavingModel) {
        startSavingModel = false;
      }
    }

    @Override
    public Boolean getLastSavingStatus() {
      return lastSavingStatus;
    }

    @Override
    public List<Long> getModelSavingList() {
      return savingModelList;
    }

    @Override
    public Boolean getContainerStarted() {
      return containerStarted;
    }

    @Override
    public String getTfEvaluatorId(){
      return tfEvaluatorContainerId;
    }

    @Override
    public String getAMContainerID(){
      return amContainerId;
    }

    @Override
    public String getContainerStdOut(HboxContainerId cid) {
      if (cid.toString().equals(amContainerId)) {
        if(amContainerStdOut.length() > 0) {
          String stdOut = amContainerStdOut.toString();
          amContainerStdOut.setLength(0);
          return stdOut;
        } else {
          return "";
        }
      }
      return containerListener.getContainerStdOut(cid);
    }

    @Override
    public String getContainerStdErr(HboxContainerId cid) {
      if (cid.toString().equals(amContainerId)) {
        if(amContainerStdErr.length() > 0) {
          String stdErr = amContainerStdErr.toString();
          amContainerStdErr.setLength(0);
          return stdErr;
        } else {
          return "";
        }
      }
      return containerListener.getContainerStdErr(cid);
    }

    @Override
    public void sendSignal(int sid){
      containerListener.sendSignal(sid);
    }

  }

  /**
   * @param args Command line args
   */
  public static void main(String[] args) {
    ApplicationMaster appMaster;
    try {
      appMaster = new ApplicationMaster();
      appMaster.init();
      if (appMaster.run()) {
        LOG.info("Application completed successfully.");
        System.exit(0);
      } else {
        LOG.info("Application failed.");
        System.exit(1);
      }
    } catch (Exception e) {
      LOG.fatal("ApplicationMaster Exit With Info: ", e);
      System.exit(1);
    }
  }

}
