package com.xz.pojo;

import java.io.FileOutputStream;
import java.util.Objects;

/**
 * @author kouyouyang
 * @date 2019-07-24 14:54
 */
public class S3WriterBean {
    private FileOutputStream fileOutputStream;
    private String day;
    private String hour;
    private String minute;
    private String second;
    private String millions;
    private String serverAddress;

    public S3WriterBean() {
    }

    public S3WriterBean(FileOutputStream fileOutputStream, String day, String hour, String minute, String second, String millions, String serverAddress) {
        this.fileOutputStream = fileOutputStream;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.millions = millions;
        this.serverAddress = serverAddress;
    }

    public FileOutputStream getFileOutputStream() {
        return fileOutputStream;
    }

    public void setFileOutputStream(FileOutputStream fileOutputStream) {
        this.fileOutputStream = fileOutputStream;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getMinute() {
        return minute;
    }

    public void setMinute(String minute) {
        this.minute = minute;
    }

    public String getSecond() {
        return second;
    }

    public void setSecond(String second) {
        this.second = second;
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof S3WriterBean)) return false;
        S3WriterBean that = (S3WriterBean) o;
        return Objects.equals(getFileOutputStream(), that.getFileOutputStream()) &&
                Objects.equals(getDay(), that.getDay()) &&
                Objects.equals(getHour(), that.getHour()) &&
                Objects.equals(getMinute(), that.getMinute()) &&
                Objects.equals(getSecond(), that.getSecond()) &&
                Objects.equals(getMillions(), that.getMillions()) &&
                Objects.equals(getServerAddress(), that.getServerAddress());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFileOutputStream(), getDay(), getHour(), getMinute(), getSecond(), getMillions(), getServerAddress());
    }

    public String getMillions() {
        return millions;
    }

    public void setMillions(String millions) {
        this.millions = millions;
    }

    @Override
    public String toString() {
        return "S3WriterBean{" +
                "fileOutputStream=" + fileOutputStream +
                ", day='" + day + '\'' +
                ", hour='" + hour + '\'' +
                ", minute='" + minute + '\'' +
                ", second='" + second + '\'' +
                ", millions='" + millions + '\'' +
                ", serverAddress='" + serverAddress + '\'' +
                '}';
    }
}
