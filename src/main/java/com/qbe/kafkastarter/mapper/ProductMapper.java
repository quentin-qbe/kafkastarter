package com.qbe.kafkastarter.mapper;

import com.qbe.avro.Product;
import com.qbe.kafkastarter.dto.ProductDto;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface ProductMapper {

    Product toAvro(ProductDto dto);

    ProductDto fromAvro(Product event);
}
