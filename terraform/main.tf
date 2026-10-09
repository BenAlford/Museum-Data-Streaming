provider "aws" {
  region = "eu-west-2"
}

data "aws_ami" "amazon_linux" {
  most_recent = true

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }

  owners = ["amazon"]
}

data "aws_vpc" "existing" {
  filter {
    name   = "tag:Name"
    values = [var.vpc_name]
  }
}

data "aws_subnet" "public_1" {
  vpc_id = data.aws_vpc.existing.id

  filter {
    name   = "tag:Name"
    values = [var.public_subnet_1_name]
  }
}

data "aws_subnet" "public_2" {
  vpc_id = data.aws_vpc.existing.id

  filter {
    name   = "tag:Name"
    values = [var.public_subnet_2_name]
  }
}

resource "aws_db_subnet_group" "default" {
  name       = "${var.rds_instance_name}-subnets"
  subnet_ids = [data.aws_subnet.public_1.id, data.aws_subnet.public_2.id]
}

resource "aws_security_group" "ec2" {
  name        = "${var.rds_instance_name}-ec2-sg"
  description = "Allow SSH access to the bastion EC2 instance"
  vpc_id      = data.aws_vpc.existing.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.ssh_allowed_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "rds" {
  name        = "${var.rds_instance_name}-sg"
  description = "Allow PostgreSQL access from the bastion EC2 instance and allowed CIDRs"
  vpc_id      = data.aws_vpc.existing.id

  ingress {
    description     = "PostgreSQL from bastion"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.ec2.id]
  }

  ingress {
    description = "PostgreSQL from allowed CIDRs"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = var.rds_allowed_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "default" {
  allocated_storage      = 20
  engine                 = "postgres"
  engine_version         = "16"
  instance_class         = var.rds_instance_type
  identifier             = var.rds_instance_name
  db_subnet_group_name   = aws_db_subnet_group.default.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = true
  db_name                = "museum"
  username               = var.db_username
  password               = var.db_password
  parameter_group_name   = "default.postgres16"
  skip_final_snapshot    = true
}

resource "aws_instance" "bastion" {
  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = var.ec2_instance_type
  subnet_id                   = data.aws_subnet.public_1.id
  vpc_security_group_ids      = [aws_security_group.ec2.id]
  key_name                    = var.key_name
  associate_public_ip_address = true

  tags = {
    Name = "${var.rds_instance_name}-bastion"
  }
}
