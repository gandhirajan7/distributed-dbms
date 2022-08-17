package com.dpgten;

import com.jcraft.jsch.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class GCPVmConnection {

    public ChannelSftp connectVM() throws JSchException, SftpException {
        JSch jSch = new JSch();
        jSch.addIdentity("/home/gandhirajan1997/id_rsa");
        Session session = jSch.getSession("avuser", "34.136.6.35", 22);
        session.setConfig("StrictHostKeyChecking", "no");
        System.out.println("Establishing Connection...");
        session.connect();
        System.out.println("Connection established.");
        System.out.println("Crating SFTP Channel.");
        ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
        sftpChannel.connect();
        System.out.println("SFTP Channel connected ...");
        sftpChannel.cd("/home/avuser");
        String pwd = sftpChannel.pwd();
        String dir = "schema";
        SftpATTRS val = null;
        try {
            val = sftpChannel.stat(pwd + "/" + dir);
        } catch (Exception exception) {
            System.out.println("Directory Not Found");
        }
        if (val != null) {
            System.out.println("Directory exists IsDir="+val.isDir());
        } else {
            sftpChannel.mkdir("/home/avuser/" + dir);
            System.out.println("Directory created !!!!!");
        }

//        List<InputStream> inputStreamList = getAllFilesFromDataBase("newDir", sftpChannel);
//        for (InputStream ip: inputStreamList) {
//
//            try {
//                BufferedReader reader = new BufferedReader(new InputStreamReader(ip));
//                String line=null;
//                while (null  != (line = reader.readLine())) {
//                    System.out.println(line);
//                }
//            } catch (IOException exception){}
//        }
//        Vector<ChannelSftp.LsEntry> entries = sftpChannel.ls("/home/avuser");
//        System.out.println("Entries in root directory:");
//        for (ChannelSftp.LsEntry entry : entries) {
//            System.out.println(entry.getFilename());
//        }
//
//        sftpChannel.disconnect();
//
//        session.disconnect();
        return sftpChannel;
    }

    public boolean checkIfDBExistsInRemoteVM(ChannelSftp sftpChannel, String selectedDatabase) throws SftpException {
        sftpChannel.cd("/home/avuser/schema");
        SftpATTRS val = null;
        try {
            val = sftpChannel.stat(sftpChannel.pwd() + "/" + selectedDatabase);
        } catch (Exception exception) {
            System.out.println("Directory Not Found");
        }
        if (val != null) {
            System.out.println("Directory exists IsDir="+val.isDir());
            return true;
        }
        return false;
    }

    public void stopGCPVMConnection(ChannelSftp sftpChannel) {
        sftpChannel.disconnect();
    }


    public void writeTORemoteFile(String databaseName, String tableName, ChannelSftp sftpChannel, String contents) throws SftpException {
        sftpChannel.put(new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8)), "/home/avuser/schema" + "/" + databaseName + "/" + tableName + ".txt");
    }


    public Map<String, InputStream> getAllFilesFromDataBase(String databaseName, ChannelSftp sftpChannel) throws SftpException {
        SftpATTRS sftpATTRS = sftpChannel.stat("/home/avuser/schema"+ "/" + databaseName);
        List<InputStream> fileList = new ArrayList<>();
        Map<String, InputStream> fileMap = new HashMap<>();
        if (sftpATTRS != null) {
            Vector<ChannelSftp.LsEntry> entries = sftpChannel.ls("/home/avuser/schema"+"/"+databaseName);
            System.out.println("Entries in root directory:");
            for (ChannelSftp.LsEntry entry : entries) {
                if (!entry.getFilename().equals("..") && !entry.getFilename().equals(".")) {
                    InputStream ip = sftpChannel.get("/home/avuser/schema" +"/" + databaseName+"/" + entry.getFilename());
                    fileList.add(ip);
                    fileMap.put(entry.getFilename(), ip);
                }
            }
        } else {
            System.out.println("Database Does Not exists !!!!. Please select the existing one.");
        }
        return fileMap;
    }

}
