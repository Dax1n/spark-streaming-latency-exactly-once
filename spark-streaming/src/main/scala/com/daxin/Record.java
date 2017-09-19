package com.daxin;

import java.io.Serializable;

/**
 * Created by Daxin on 2017/8/31.
 */
public class Record implements Serializable {


    private String mmac;

    private String id;

    private String time;
    private String mac;

    public void setId(String id) {
        this.id = id;
    }

    public void setMmac(String mmac) {
        this.mmac = mmac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }


    public void setTime(String time) {
        this.time = time;
    }

    public String getId() {
        return id;
    }


    public String getMac() {
        return mac;
    }


    public String getMmac() {
        return mmac;
    }


    public String getTime() {
        return time;
    }



}
