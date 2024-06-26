package net.qihoo.hbox.common;

import net.qihoo.hbox.conf.HboxConfiguration;
import net.qihoo.hbox.container.HboxContainerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public class DockerLaunch implements ILaunch {

    private static final Log LOG = LogFactory.getLog(DockerLaunch.class);
    private String containerId;
    private Process hboxProcess;
    private HboxConfiguration conf;
    private String runArgs;
    private String gpu;
    private String dockerType;

    public DockerLaunch(String containerId, HboxConfiguration conf) {
        this.containerId = containerId;
        this.conf = conf;
        this.gpu = "";
        this.runArgs = conf.get(HboxConfiguration.HBOX_DOCKER_RUN_ARGS, "");
        this.dockerType = "docker";
    }

    @Override
    public Process exec(String[] commandArgs, String[] envp, Map<String, String> envs, File dir) throws IOException {
        return exec(String.join(" ", commandArgs), envp, envs, dir);
    }

    @Override
    /**
     * @para envp 作业的相关环境变量，包括index，rolo，path作业的一些参数等
     * @pare envs 当前系统的环境变量，本机的本地环境D
     */
    public Process exec(String command, String[] envp, Map<String, String> envs, File dir) throws IOException {
        LOG.info("docker command:" + command + ",envs:" + envs);
        Runtime rt = Runtime.getRuntime();
        String port = conf.get("RESERVED_PORT", "");
        String containerPort = conf.get("DOCKER_PORT", "");
        String workDir = "/" + conf.get(HboxConfiguration.HBOX_DOCKER_WORK_DIR, HboxConfiguration.DEFAULT_HBOX_DOCKER_WORK_DIR);
        String path = new File("").getAbsolutePath();
        StringBuilder envsParam = new StringBuilder();
        //把container的环境变量添加到docker中去
        for (String keyValue : envp) {
            if (keyValue.startsWith("PATH") || keyValue.startsWith("CLASSPATH")) {
                continue;
            } else if (keyValue.startsWith("NV_GPU")) {
                String[] cudaDevice = keyValue.split("=");
                LOG.info("cudaDevice length: " + cudaDevice.length);
                if (cudaDevice.length == 2) {
                    gpu = cudaDevice[1];
                    dockerType = "nvidia-docker";
                }
            } else {
                envsParam.append(" --env " + keyValue + "");
            }
        }
        //xdl 作业指定zk端口
        if (port != null && !port.trim().equals("")) {
            if (containerPort != null && !containerPort.trim().equals(""))
                port = " -p " + port + ":" + containerPort;
            else
                port = " -p " + port;
        }
        String containerMemory = envs.get("DOCKER_CONTAINER_MEMORY");
        String containerCpu = envs.get("DOCKER_CONTAINER_CPU");
        String network = conf.get("DOCKER_CONTAINER_NETWORK", "");
        String userName = conf.get("hadoop.job.ugi");
        String[] userNameArr = userName.split(",");
        if (userNameArr.length > 1) {
            userName = userNameArr[0];
        }
        LOG.info("Container launch userName:" + userName);
        String user = conf.get("DOCKER_CONTAINER_USER", userName);
        String mount = " -v " + path + ":" + workDir;
        mount = mount + " -v /etc/passwd:/etc/passwd:ro";
        String homePath = envs.get("HADOOP_HDFS_HOME");
        if (homePath != null && homePath != "")
            mount += " -v " + homePath + ":" + homePath + ":ro";
        String javaPath = envs.get("JAVA_HOME");
        if (javaPath != null && javaPath != "")
            mount += " -v " + javaPath + ":" + javaPath + ":ro";
        String[] localDirs = envs.get("LOCAL_DIRS").split(",");
        Boolean publicFlag = conf.get(HboxConfiguration.HBOX_LOCAL_RESOURCE_VISIBILITY, HboxConfiguration.DEFAULT_HBOX_LOCAL_RESOURCE_VISIBILITY).equalsIgnoreCase("public");
        if (localDirs.length > 0) {
            for (String perPath : localDirs) {
                if (publicFlag) {
                    String[] localPath = perPath.split("usercache");
                    mount = mount + " -v " + localPath[0] + "filecache" + ":" + localPath[0] + "filecache";
                } else {
                    mount = mount + " -v " + perPath + ":" + perPath;
                }
            }
        }
        String[] logsDirs = envs.get("LOG_DIRS").split(",");
        if (localDirs.length > 0) {
            for (String perPath : logsDirs) {
                mount = mount + " -v " + perPath + ":" + perPath;
            }
        }

        String dockerImageName = conf.get(HboxConfiguration.HBOX_DOCKER_IMAGE_NAME);
        //从仓库拉取镜像文件
        try {
            String dockerPullCommand = "docker pull " + dockerImageName;
            LOG.info("Docker Pull command:" + dockerPullCommand);
            Process process = rt.exec(dockerPullCommand, envp);
            int i = process.waitFor();
            LOG.info("Docker Pull Wait:" + (i == 0 ? "Success" : "Failed"));
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                LOG.info(line);
            }
        } catch (InterruptedException e) {
            LOG.warn("Docker pull Error:", e);
        }

        String dockerCommand = dockerType + " run";
        //如果提交用户不是root的话，使用指定的用户启动docker作业
        if (!user.equalsIgnoreCase("root")) {
            String userId = "";
            try {
                String userIDCommand = "id -u";
                LOG.info("Get the user id :" + userIDCommand);
                Process process = rt.exec(userIDCommand, envp);
                int i = process.waitFor();
                LOG.info("Get the user id Wait:" + (i == 0 ? "Success" : "Failed"));
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String line;
                while ((line = br.readLine()) != null) {
                    LOG.info(line);
                    userId = line;
                }
            } catch (InterruptedException e) {
                LOG.warn("Get the user id error:", e);
            }
            if (userId.trim() != "") {
                dockerCommand += " -u " + userId;
            }
        }

        if (network != null && network.equalsIgnoreCase("host")) {
            dockerCommand += " --network host";
        }
        dockerCommand +=
                " --rm " +
                        " --cpus " + containerCpu +
                        " -m " + containerMemory + "m " +
                        port +
                        " -w " + workDir +
                        mount +
                        envsParam.toString() +
                        " --name " + containerId + " " +
                        runArgs + " " +
                        dockerImageName;
        dockerCommand += " " + command;
        LOG.info("Docker command:" + dockerCommand);
        hboxProcess = rt.exec(dockerCommand, envp);

        return hboxProcess;
    }
}
