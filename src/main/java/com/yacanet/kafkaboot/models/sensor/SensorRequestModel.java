package com.yacanet.kafkaboot.models.sensor;

public class SensorRequestModel {
    /**
     * sensor id
     */
    private String sensorId;
    /**
     * isi data dari sensor
     */
    private String data;

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String id) {
        this.sensorId = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
