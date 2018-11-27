package com.zsk.MapReduce.domain;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class StudentinfoRecord implements Writable, DBWritable {
    private int id;
    private String name;

    public static String[] FIELDS = new String[]{
            "id",
            "name"
    };

    @Override
    public void readFields(ResultSet resultSet) throws SQLException{
        this.id= resultSet.getInt(1);
        this.name = resultSet.getString(2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException{
        this.id = dataInput.readInt();
        this.name = dataInput.readUTF();
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException{
        preparedStatement.setInt(1,this.id);
        preparedStatement.setString(2,this.name);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.name);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public StudentinfoRecord(int id, String name) {
        this.id = id;
        this.name = name;
    }
}
