package com.qbe.kafkastarter.mapper;

import com.qbe.avro.Stock;
import com.qbe.kafkastarter.dto.StockDto;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface StockMapper {

    Stock toAvro(StockDto dto);

    StockDto fromAvro(Stock event);
}
